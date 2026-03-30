from dotenv import load_dotenv
import os

load_dotenv()

OUTPUT_DIR = "./output_files"
CACHE_DIR = f'{OUTPUT_DIR}/cache'
LOG_DIR = f'{OUTPUT_DIR}/logs'

API_KEY = os.getenv("PATSTAT_API_KEY")
PATENT_URL = "https://search.patentsview.org/api/v1/patent/"
INVENTOR_URL = "https://search.patentsview.org/api/v1/inventor/"
ASSIGNEE_URL = "https://search.patentsview.org/api/v1/assignee/"

START_YEAR = 1970
END_YEAR = 2020

PER_PAGE = 1000
SAVE_QUERY = 25000
ENTITY_PART_SIZE = 100
SAVE_EVERY = 500

YEAR_FILTER = {
    "_and": [
        {"_gte": {"patent_year": START_YEAR}},
        {"_lte": {"patent_year": END_YEAR}}
    ]
}

# OLD_CPC_CODES = [
#     "Y02W30", # E-waste recycling & Urban mining
#     "C22B7", "C22B19", # Material recovery / non-ferrous extraction
#     "B09B3", "Y02W10" # Circular economy / reuse / waste treatment
# ]

CPC_CODES = [
    "Y02W30",   # Waste management & recycling technologies, incl. WEEE and plastics recycling [web:3][web:15][web:92]
    "Y02W10",   # Waste collection, transportation, storage (enabling circular waste systems) [web:121]
    "C22B1",    # Preliminary treatment of ores or scrap for subsequent metal extraction [web:38]
    "C22B3",    # Wet processes (e.g. leaching) for extracting metal compounds [web:38]
    "C22B7",    # Working up raw materials other than ores, incl. metal scrap [web:38]
    "C22B11",   # Recovery of noble metals such as Au, Ag, PGMs [web:38]
    "C22B15",   # Obtaining copper from ores or scrap (e.g. from PCBs, cables) [web:38]
    "C22B19",   # Obtaining zinc or zinc oxide from ores or scrap [web:38]
    "C22B59",   # Obtaining rare earth metals from ores or scrap, incl. e-waste [web:38][web:55]
    "C25C1",    # Electrolytic production, recovery or refining of metals from solutions [web:102]
    "B09B3",    # Disposal, destruction or transformation of solid waste into useful/harmless products [web:24][web:118]
    "B09B2101", # Indexing codes specifying type of waste (electronic waste, batteries, PCBs, etc.) [web:24]
    "B03B9",    # Separating or washing apparatus specially adapted for refuse/industrial waste [web:71][web:74]
]

# OLD_KEYOWRDS = [
#     "urban mining", "e-waste recycling",
#     "metal recovery", "circular economy",
#     "waste treatment", "resource recovery"
# ]

KEYWORDS = [
    "urban mining",
    "electronic waste",
    "e-waste recycling", 
    "electronic scrap",
    "metal recovery",
    "resource recovery",
    "circular economy",
    "rare earth",
    "printed circuit",
    "PCB recycling",
    "battery recycling",
    "plastic recovery",
    "waste treatment",
    "critical metals",
    "leaching process"
]

PATENT_FIELDS = [
    "patent_id",
    "patent_title",
    "patent_date",
    "patent_year",
    "patent_type",
    "cpc_current",
    "inventors",
    "assignees",
    "application"
]

INVENTOR_FIELDS = [
    "inventor_id",
    "inventor_name_first",
    "inventor_name_last",
    "inventor_lastknown_city",
    "inventor_lastknown_state",
    "inventor_lastknown_country",
    "inventor_lastknown_latitude",
    "inventor_lastknown_longitude"
]

ASSIGNEE_FIELDS = [
    "assignee_id",
    "assignee_organization",
    "assignee_type",
    "assignee_city",
    "assignee_state",
    "assignee_country",
    "assignee_latitude",
    "assignee_longitude"
]

ANALYSIS_FIELDS = [
    "patent_id",
    "patent_type",
    "patent_year",
    "patent_date",
    "assignee_id",
    "assignee_type",
    "assignee_organization",
    "assignee_location_id",
    "assignee_city",
    "assignee_state",
    "assignee_country",
    "inventor_id",
    "inventor_name_first",
    "inventor_name_last",
    "inventor_gender_code",
    "inventor_location_id",
    "inventor_city",
    "inventor_state",
    "inventor_country",
    "cpc_class_id",
    "cpc_subclass_id",
    "cpc_group_id",
    "application_id",
    "filing_date"
]