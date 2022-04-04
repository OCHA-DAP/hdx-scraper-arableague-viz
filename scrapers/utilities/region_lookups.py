import logging

from hdx.utilities.dictandlist import dict_of_sets_add

logger = logging.getLogger(__name__)


class RegionLookups:
    iso3_to_region = dict()
    regions = None

    @classmethod
    def load(cls, countries, hrp_countries):
        cls.regions = ("ALL", "HRP")
        for countryiso in hrp_countries:
            dict_of_sets_add(cls.iso3_to_region, countryiso, "HRP")
        for countryiso in countries:
            dict_of_sets_add(cls.iso3_to_region, countryiso, "ALL")
