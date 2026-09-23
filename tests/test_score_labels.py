"""Tests for the agreement metrics in scripts/score_labels.py."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    "score_labels", Path(__file__).resolve().parents[1] / "scripts" / "score_labels.py"
)
score_labels = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(score_labels)


def test_perfect_agreement_is_kappa_one():
    pairs = [("Low", "Low"), ("High", "High"), ("Medium", "Medium")]
    assert score_labels.cohens_kappa(pairs) == pytest.approx(1.0)
    assert score_labels.cohens_kappa(pairs, "quadratic") == pytest.approx(1.0)


def test_kappa_matches_hand_computation():
    # p_o = 0.75; marginals human (L .5, M .5), model (L .25, M .75) -> p_e = 0.5
    # kappa = (0.75 - 0.5) / (1 - 0.5) = 0.5. With only adjacent tiers in play,
    # quadratic weighting scales numerator and denominator equally -> also 0.5.
    pairs = [("Low", "Low"), ("Low", "Medium"), ("Medium", "Medium"), ("Medium", "Medium")]
    assert score_labels.cohens_kappa(pairs) == pytest.approx(0.5)
    assert score_labels.cohens_kappa(pairs, "quadratic") == pytest.approx(0.5)


def test_quadratic_kappa_penalises_distant_disagreement_more():
    near = [("Low", "Low"), ("High", "High"), ("Medium", "High"), ("Critical", "Critical")]
    far = [("Low", "Low"), ("High", "High"), ("Low", "Critical"), ("Critical", "Critical")]
    assert score_labels.cohens_kappa(near, "quadratic") > score_labels.cohens_kappa(far, "quadratic")


@pytest.mark.parametrize("raw,expected", [("high", "High"), (" LOW ", "Low"), ("Critical", "Critical")])
def test_labels_are_normalised(raw, expected):
    assert score_labels._normalise(raw) == expected


def test_unknown_label_rejected():
    with pytest.raises(ValueError):
        score_labels._normalise("Urgent")
