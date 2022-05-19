import logging

from hdx.location.adminone import AdminOne
from hdx.location.country import Country
from hdx.scraper.configurable.aggregator import Aggregator
from hdx.scraper.outputs.update_tabs import (
    get_regional_rows,
    get_toplevel_rows,
    update_national,
    update_regional,
    update_sources,
    update_subnational,
    update_toplevel,
)
from hdx.scraper.runner import Runner

from .covax_deliveries import CovaxDeliveries
from .education_closures import EducationClosures
from .education_enrolment import EducationEnrolment
from .food_prices import FoodPrices
from .fts import FTS
from .inform import Inform
from .iom_dtm import IOMDTM
from .ipc import IPC
from .unhcr import UNHCR
from .utilities.region_lookups import RegionLookups
from .vaccination_campaigns import VaccinationCampaigns
from .who_covid import WHOCovid
from .whowhatwhere import WhoWhatWhere

logger = logging.getLogger(__name__)


def get_indicators(
    configuration,
    today,
    outputs,
    tabs,
    scrapers_to_run=None,
    countries_override=None,
    errors_on_exit=None,
    use_live=True,
):
    Country.countriesdata(
        use_live=use_live,
        country_name_overrides=configuration["country_name_overrides"],
        country_name_mappings=configuration["country_name_mappings"],
    )

    if countries_override:
        countries = countries_override
    else:
        countries = configuration["countries"]
    hrp_countries = configuration["HRPs"]
    configuration["countries_fuzzy_try"] = countries
    adminone = AdminOne(configuration)
    regional_configuration = configuration["regional"]
    RegionLookups.load(regional_configuration, today, countries, hrp_countries)
    runner = Runner(
        countries,
        adminone,
        today,
        errors_on_exit=errors_on_exit,
        scrapers_to_run=scrapers_to_run,
    )
    configurable_scrapers = dict()
    for level_name in "national", "subnational", "allregions":
        if level_name == "allregions":
            level = "single"
        else:
            level = level_name
        suffix = f"_{level_name}"
        configurable_scrapers[level_name] = runner.add_configurables(
            configuration[f"scraper{suffix}"], level, level_name, suffix=suffix
        )
    who_covid = WHOCovid(
        configuration["who_covid"],
        today,
        outputs,
        countries,
    )
    ipc = IPC(configuration["ipc"], today, countries, adminone)

    fts = FTS(configuration["fts"], today, outputs, countries)
    food_prices = FoodPrices(configuration["food_prices"], today, countries)
    vaccination_campaigns = VaccinationCampaigns(
        configuration["vaccination_campaigns"],
        today,
        countries,
        outputs,
    )
    unhcr = UNHCR(configuration["unhcr"], today, countries)
    inform = Inform(configuration["inform"], today, countries)
    covax_deliveries = CovaxDeliveries(
        configuration["covax_deliveries"], today, countries
    )
    education_closures = EducationClosures(
        configuration["education_closures"],
        today,
        countries,
        RegionLookups.iso3_to_region,
    )
    education_enrolment = EducationEnrolment(
        configuration["education_enrolment"],
        education_closures,
        countries,
        RegionLookups.iso3_to_region,
    )
    national_names = configurable_scrapers["national"] + [
        "food_prices",
        "vaccination_campaigns",
        "fts",
        "unhcr",
        "inform",
        "ipc",
        "covax_deliveries",
        "education_closures",
        "education_enrolment",
    ]
    national_names.insert(1, "who_covid")

    whowhatwhere = WhoWhatWhere(configuration["whowhatwhere"], today, adminone)
    iomdtm = IOMDTM(configuration["iom_dtm"], today, adminone)

    subnational_names = configurable_scrapers["subnational"] + [
        "whowhatwhere",
        "iom_dtm",
    ]
    subnational_names.insert(1, "ipc")

    runner.add_customs(
        (
            who_covid,
            ipc,
            fts,
            food_prices,
            vaccination_campaigns,
            unhcr,
            inform,
            covax_deliveries,
            education_closures,
            education_enrolment,
            whowhatwhere,
            iomdtm,
        )
    )

    regional_scrapers = Aggregator.get_scrapers(
        regional_configuration["aggregate"],
        "national",
        "regional",
        RegionLookups.iso3_to_region,
        runner,
    )
    regional_names = runner.add_customs(regional_scrapers, add_to_run=True)
    regional_names.extend(["education_closures", "education_enrolment"])

    runner.run(
        prioritise_scrapers=(
            "population_national",
            "population_subnational",
            "population_allregions",
        )
    )

    if "national" in tabs:
        flag_countries = {
            "header": "ishrp",
            "hxltag": "#meta+ishrp",
            "countries": hrp_countries,
        }
        update_national(
            runner,
            countries,
            outputs,
            names=national_names,
            flag_countries=flag_countries,
            iso3_to_region=RegionLookups.iso3_to_region,
            ignore_regions=("ALL",),
        )
    regional_rows = get_regional_rows(
        runner,
        RegionLookups.regions,
        names=regional_names,
    )
    if "regional" in tabs:
        update_regional(
            outputs,
            regional_rows,
        )
    if "allregions" in tabs:
        allregions_names = configurable_scrapers["allregions"]
        allregions_rows = get_toplevel_rows(runner, names=allregions_names)
        update_toplevel(
            outputs, allregions_rows, regional_rows=regional_rows, regional_first=True
        )
    if "subnational" in tabs:
        update_subnational(runner, adminone, outputs, names=subnational_names)

    adminone.output_matches()
    adminone.output_ignored()
    adminone.output_errors()

    if "sources" in tabs:
        update_sources(runner, configuration, outputs)
    return countries
