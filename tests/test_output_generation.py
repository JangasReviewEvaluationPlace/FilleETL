import os
import filecmp
from amazon_reviews import ETL as AmazonReviewETL
from etl.utils import BaseETL


FILE_DIRE = os.path.dirname(os.path.realpath(__file__))


class TestCSVOutputs:
    def setup_method(self):
        pass

    def teardown_method(self):
        pass

    def cleanup_output_directory(self, etl: BaseETL):
        already_existing_output_files = [
            f for f in os.listdir(etl.output_dir) if f.endswith(".csv")
        ]
        for f in already_existing_output_files:
            if "amazon_reviews" in f:
                os.remove(os.path.join(etl.output_dir, f))

    def test_amazon_review_singlethread_no_chunking(self):
        etl = AmazonReviewETL(is_dummy=True)
        etl.sample_data_dir = os.path.join(FILE_DIRE, "fixtures", "sample_data", "amazon_reviews")
        self.cleanup_output_directory(etl=etl)
        etl.run()
        created_file_path = os.path.join(etl.output_dir, "amazon_reviews.csv")
        fixture_file_path = os.path.join(FILE_DIRE, "fixtures", "expected_outputs",
                                         "amazon_reviews.csv")
        assert filecmp.cmp(created_file_path, fixture_file_path)
