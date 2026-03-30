from pipelines.step1_data_extraction import run as step1_data_extraction
from pipelines.step2_data_clean import run as step2_data_cleaning
from pipelines.step3_data_processing import run as step3_data_processing

if __name__ == "__main__":
    # step1_data_extraction()
    # step2_data_cleaning()
    step3_data_processing()