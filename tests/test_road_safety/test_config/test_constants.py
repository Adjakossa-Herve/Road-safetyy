from __future__ import annotations

from road_safety.config.constants import COMMUNE_CORRECTIONS, LUMINOSITY_CORRECTIONS


def test_commune_corrections_contains_expected_aliases():
    assert COMMUNE_CORRECTIONS["Non renseignee nterre"] == "Nanterre"
    assert COMMUNE_CORRECTIONS["Non renseignee"] == "Unknown"


def test_luminosity_corrections_contains_expected_alias():
    assert (
        LUMINOSITY_CORRECTIONS["Nuit sanseclairage public"]
        == "Nuit sans eclairage public"
    )
