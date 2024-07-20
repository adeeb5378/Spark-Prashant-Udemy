from unittest import TestCase
from pyspark.sql import SparkSession
from lib.utils import load_cricket_df, countNoOfPlayingRole


class UtilsTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .master("local[3]") \
            .appName("HelloSparkTest") \
            .getOrCreate()

    def test_datafile_loading(self):
        sample_df = load_cricket_df(self.spark,"data/players_info.csv")
        result_count = sample_df.count()
        self.assertEquals(result_count,219,"Record count is true")

    def test_playing_role_count(self):
        sample_df = load_cricket_df(self.spark, "data/players_info.csv")
        count_list = countNoOfPlayingRole(sample_df).collect()
        count_dict = dict()
        for row in count_list:
            count_dict[row["PlayingRole"]] = row["count"]

        self.assertEquals(count_dict["Bowling Allrounder"],16,"Count of Bowling AllRounder is True")
        self.assertEquals(count_dict["Batting Allrounder"], 15, "Count of Batting AllRounder is True ")


    # @classmethod
    # def tearDownClass(cls) -> None:
    #     cls.spark.stop()

