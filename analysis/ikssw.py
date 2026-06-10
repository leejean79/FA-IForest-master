#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IKSSW — Python port of the Java IKS / IKSConfig deployed under
com.leejean.drift (see src/main/java/com/leejean/drift/IKS.java).

Faithful mirror of the composite-key lex IKS used in production, so per-feature
fire decisions in the Phase-1 de-risk harness reflect what the Flink job would
see at runtime. Treap node fields (key/value/priority/lazy/maxValue/minValue)
and Add/Remove operations are byte-for-byte translations of the Java port.

Public API matching HANDOVER §4:
    ca = CAForPValue(p)             # sqrt(-0.5 * ln(p))
    ikssw = IKSSW(reference_window) # warm-up reference, also seeds current
    ikssw.Increment(v)              # slide one new value into current
    ikssw.KS()                      # max(maxValue, -minValue) / W
    ikssw.Test(ca)                  # KS() > ca * sqrt(2/W)
    ikssw.Update()                  # reference <- current sliding (rebase)
"""
from __future__ import annotations

import math
import random
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Optional, Tuple, Iterable


_GROUP_CURRENT = 0
_GROUP_REFERENCE = 1


# ---------------- composite key ----------------

@dataclass(frozen=True)
class IKSKey:
    """(value, rnd, group) — lex order. rnd = per-insert random tiebreak."""
    value: float
    rnd: int
    group: int

    def __lt__(self, other: "IKSKey") -> bool:
        if self.value != other.value:
            return self.value < other.value
        if self.rnd != other.rnd:
            return self.rnd < other.rnd
        return self.group < other.group

    def __le__(self, other: "IKSKey") -> bool:
        return self == other or self < other


# ---------------- Treap node ----------------

class Treap:
    """Treap with lazy range-add + subtree min/max. Port of Treap.java."""
    __slots__ = ("key", "value", "priority", "lazy",
                 "maxValue", "minValue", "size", "left", "right")

    def __init__(self, key: IKSKey, value: int = 0):
        self.key = key
        self.value = value
        self.priority = random.random()
        self.lazy = 0
        self.maxValue = value
        self.minValue = value
        self.size = 1
        self.left: Optional[Treap] = None
        self.right: Optional[Treap] = None


def size(node: Optional[Treap]) -> int:
    return 0 if node is None else node.size


def sumAll(node: Optional[Treap], delta: int) -> None:
    if node is None or delta == 0:
        return
    node.value += delta
    node.maxValue += delta
    node.minValue += delta
    node.lazy += delta


def unlazy(node: Optional[Treap]) -> None:
    if node is None or node.lazy == 0:
        return
    sumAll(node.left, node.lazy)
    sumAll(node.right, node.lazy)
    node.lazy = 0


def update(node: Optional[Treap]) -> None:
    if node is None:
        return
    unlazy(node)
    s = 1
    mx = node.value
    mn = node.value
    if node.left is not None:
        s += node.left.size
        if node.left.maxValue > mx:
            mx = node.left.maxValue
        if node.left.minValue < mn:
            mn = node.left.minValue
    if node.right is not None:
        s += node.right.size
        if node.right.maxValue > mx:
            mx = node.right.maxValue
        if node.right.minValue < mn:
            mn = node.right.minValue
    node.size = s
    node.maxValue = mx
    node.minValue = mn


def splitKeepRight(node: Optional[Treap],
                   key: IKSKey) -> Tuple[Optional[Treap], Optional[Treap]]:
    """left = {< key}, right = {>= key}. Equal keys go right."""
    if node is None:
        return None, None
    unlazy(node)
    if key <= node.key:
        sub_left, sub_right = splitKeepRight(node.left, key)
        node.left = sub_right
        update(node)
        return sub_left, node
    else:
        sub_left, sub_right = splitKeepRight(node.right, key)
        node.right = sub_left
        update(node)
        return node, sub_right


def merge(left: Optional[Treap], right: Optional[Treap]) -> Optional[Treap]:
    if left is None:
        return right
    if right is None:
        return left
    if left.priority > right.priority:
        unlazy(left)
        left.right = merge(left.right, right)
        update(left)
        return left
    else:
        unlazy(right)
        right.left = merge(left, right.left)
        update(right)
        return right


def splitSmallest(node: Optional[Treap]) -> Tuple[Optional[Treap], Optional[Treap]]:
    if node is None:
        return None, None
    unlazy(node)
    if node.left is None:
        rest = node.right
        node.right = None
        update(node)
        return node, rest
    sub_left, sub_right = splitSmallest(node.left)
    node.left = sub_right
    update(node)
    return sub_left, node


def splitGreatest(node: Optional[Treap]) -> Tuple[Optional[Treap], Optional[Treap]]:
    if node is None:
        return None, None
    unlazy(node)
    if node.right is None:
        rest = node.left
        node.left = None
        update(node)
        return rest, node
    sub_left, sub_right = splitGreatest(node.right)
    node.right = sub_left
    update(node)
    return node, sub_right


# ---------------- IKS / IKSSW ----------------

class _IKS:
    """Two-group IKS treap (port of IKS.java add/remove/ks)."""

    def __init__(self):
        self.treap: Optional[Treap] = None
        self.n_current = 0
        self.n_reference = 0

    def add(self, key: IKSKey, group: int) -> None:
        if group == _GROUP_CURRENT:
            self.n_current += 1
        else:
            self.n_reference += 1
        left, right = splitKeepRight(self.treap, key)
        left, leftG = splitGreatest(left)
        val = 0 if leftG is None else leftG.value
        left = merge(left, leftG)
        right = merge(Treap(key, val), right)
        sumAll(right, +1 if group == _GROUP_CURRENT else -1)
        self.treap = merge(left, right)

    def remove(self, key: IKSKey, group: int) -> None:
        if group == _GROUP_CURRENT:
            self.n_current -= 1
        else:
            self.n_reference -= 1
        left, right = splitKeepRight(self.treap, key)
        rightL, right = splitSmallest(right)
        if rightL is not None and rightL.key == key:
            sumAll(right, -1 if group == _GROUP_CURRENT else +1)
            # discard rightL
        else:
            right = merge(rightL, right)
        self.treap = merge(left, right)

    def ks(self) -> float:
        if self.n_current != self.n_reference:
            raise RuntimeError("ks() requires n_current == n_reference")
        W = self.n_current
        if W == 0 or self.treap is None:
            return 0.0
        peak = max(self.treap.maxValue, -self.treap.minValue)
        return peak / float(W)


class IKSSW:
    """
    IKSSW wrapper — frozen reference window + sliding current window.
    Mirrors the deployed Java IKS detector (5a STABLE/DRIFT semantics).
    """

    def __init__(self, reference_window: Iterable[float],
                 rng: Optional[random.Random] = None):
        vals = [float(v) for v in reference_window]
        self.W = len(vals)
        if self.W == 0:
            raise ValueError("reference_window must be non-empty")
        self._rng = rng or random.Random()
        self._iks = _IKS()
        self._sliding: Deque[IKSKey] = deque()
        # Warm-up flush — match HANDOVER §3.5: for each value, one rnd each,
        # add reference first, then current; push current key into sliding deque.
        for v in vals:
            ref_key = IKSKey(v, self._rng.getrandbits(63), _GROUP_REFERENCE)
            self._iks.add(ref_key, _GROUP_REFERENCE)
            cur_key = IKSKey(v, self._rng.getrandbits(63), _GROUP_CURRENT)
            self._iks.add(cur_key, _GROUP_CURRENT)
            self._sliding.append(cur_key)

    def Increment(self, value: float) -> None:
        oldest = self._sliding.popleft()
        self._iks.remove(oldest, _GROUP_CURRENT)
        new_key = IKSKey(float(value), self._rng.getrandbits(63), _GROUP_CURRENT)
        self._iks.add(new_key, _GROUP_CURRENT)
        self._sliding.append(new_key)

    def KS(self) -> float:
        return self._iks.ks()

    def Test(self, ca: float) -> bool:
        return self.KS() > ca * math.sqrt(2.0 / self.W)

    def Update(self) -> None:
        """Rebase: reference <- current sliding (for rebase_on_fire)."""
        # Snapshot current values in order, then rebuild from scratch.
        current_values = [k.value for k in self._sliding]
        self._iks = _IKS()
        self._sliding = deque()
        for v in current_values:
            ref_key = IKSKey(v, self._rng.getrandbits(63), _GROUP_REFERENCE)
            self._iks.add(ref_key, _GROUP_REFERENCE)
            cur_key = IKSKey(v, self._rng.getrandbits(63), _GROUP_CURRENT)
            self._iks.add(cur_key, _GROUP_CURRENT)
            self._sliding.append(cur_key)


def CAForPValue(p: float) -> float:
    """ca = sqrt(-0.5 * ln(p)). Matches IKSConfig.ca (Java)."""
    return math.sqrt(-0.5 * math.log(p))
