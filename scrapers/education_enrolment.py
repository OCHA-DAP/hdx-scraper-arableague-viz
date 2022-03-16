import logging

from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.utilities.readers import read
from hdx.utilities.text import get_fraction_str

logger = logging.getLogger(__name__)


class EducationEnrolment(BaseScraper):
    def __init__(self, datasetinfo, closures, countryiso3s, downloader):
        super().__init__(
            "education_enrolment",
            datasetinfo,
            {
                "national": (
                    (
                        "No. pre-primary to upper-secondary learners",
                        "No. tertiary learners",
                        "No. affected learners",
                    ),
                    (
                        "#population+learners+pre_primary_to_secondary",
                        "#population+learners+tertiary",
                        "#affected+learners",
                    ),
                ),
                "regional": (
                    (
                        "No. affected learners",
                        "Percentage affected learners",
                    ),
                    (
                        "#affected+learners",
                        "#affected+learners+pct",
                    ),
                ),
            },
        )
        self.closures = closures
        self.countryiso3s = countryiso3s
        self.downloader = downloader

    def run(self) -> None:
        learners_headers, learners_iterator = read(self.downloader, self.datasetinfo)
        learners_012, learners_3, affected_learners = self.get_values("national")
        affected_learners_total, percentage_affected_learners = self.get_values(
            "regional"
        )
        affected_learners_total["value"] = 0
        learners_total = 0
        for row in learners_iterator:
            countryiso = row["ISO3"]
            if not countryiso or countryiso not in self.countryiso3s:
                continue
            l_0 = row["Pre-primary (both)"]
            l_1 = row["Primary (both)"]
            l_2 = row["Secondary (both)"]
            l_3 = row["Tertiary (both)"]
            l_012 = None
            if l_0 is not None and l_0 != "-":
                l_012 = int(l_0)
            if l_1 is not None and l_1 != "-":
                l_1 = int(l_1)
                if l_012 is None:
                    l_012 = l_1
                else:
                    l_012 += l_1
            if l_2 is not None and l_2 != "-":
                l_2 = int(l_2)
                if l_012 is None:
                    l_012 = l_2
                else:
                    l_012 += l_2
            if l_012 is not None:
                learners_012[countryiso] = l_012
            if l_3 == "-":
                l_3 = None
            elif l_3 is not None:
                l_3 = int(l_3)
                learners_3[countryiso] = l_3
            no_learners = None
            if l_012 is not None:
                no_learners = l_012
                if l_3:
                    no_learners += l_3
            elif l_3 is not None:
                no_learners = l_3
            if no_learners is not None:
                learners_total += no_learners
                if countryiso in self.closures.fully_closed:
                    affected_learners_total["value"] += no_learners
        percentage_affected_learners["value"] = get_fraction_str(
            affected_learners_total["value"], learners_total
        )
