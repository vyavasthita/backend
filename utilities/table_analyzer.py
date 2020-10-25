import pandas as pd
import boto3
import pathlib
import json
from datetime import datetime
import random
import os


class Db2Analyzer():
    def __init__(self):
        self.output_path = None

        self.file_1 = None
        self.file_2 = None
        self.file_3 = None
        self.file_4 = None
        self.file_5 = None

        self.file_object = None

    def download_files(self):

        root_path = pathlib.Path(__file__).parent.absolute()

        input_path = os.path.join(root_path, 'input')
        self.output_path = os.path.join(root_path, 'output')

        BUCKET_NAME = 'tablemetadata'  # replace with your bucket name

        LOCAL_FILE_1 = 'TBQA2_ALL_TAB_DETAILS_10-15-2020.csv'
        LOCAL_FILE_2 = 'TBQA2_ALL_TAB_PRMRY_KEY_DETAILS_10-15-2020.csv'
        LOCAL_FILE_3 = 'TBQA2_ALL_TAB_INDEX_DETAILS_10-15-2020.csv'
        LOCAL_FILE_4 = 'TBQA2_ALL_TAB_COLUMN_DETAILS_10-15-2020.csv'
        LOCAL_FILE_5 = 'TBQA2_ALL_TAB_FRGN_KEY_DETAILS_10-15-2020.csv'

        self.file_1 = os.path.join(input_path, LOCAL_FILE_1)
        self.file_2 = os.path.join(input_path, LOCAL_FILE_2)
        self.file_3 = os.path.join(input_path, LOCAL_FILE_3)
        self.file_4 = os.path.join(input_path, LOCAL_FILE_4)
        self.file_5 = os.path.join(input_path, LOCAL_FILE_5)

        AWS_S3_CREDS = {
            "aws_access_key_id": "AKIAIVMEG5M24AHF3FWA",  # os.getenv("AWS_ACCESS_KEY")
            "aws_secret_access_key": "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"  # os.getenv("AWS_SECRET_KEY")
        }
        s3_client = boto3.client('s3', **AWS_S3_CREDS)

        s3_client.download_file(BUCKET_NAME, LOCAL_FILE_1, self.file_1)
        s3_client.download_file(BUCKET_NAME, LOCAL_FILE_2, self.file_2)
        s3_client.download_file(BUCKET_NAME, LOCAL_FILE_3, self.file_3)
        s3_client.download_file(BUCKET_NAME, LOCAL_FILE_4, self.file_4)
        s3_client.download_file(BUCKET_NAME, LOCAL_FILE_5, self.file_5)

    def db2_analyzer(self):
        """
        DB2 analyzer
        """
        try:
            json_dict = {}
            now = datetime.now()  # datetime object containing current date and time
            dt_string = now.strftime("%d-%m-%Y %H_%M_%S")  # dd-mm-YY H_M_S
            json_name = 'db2_analyzer_%s.json' % dt_string
            output_json = os.path.join(self.output_path, json_name)

            self.file_object = open(output_json, "a")  # create json file with appendba
            df_db2_table_details = pd.read_csv(self.file_1,
                                               skipinitialspace=True)  # skip whitespace in header
            df_db2_primary_key = pd.read_csv(self.file_2,
                                             skipinitialspace=True)  # skip whitespace in header
            df_db2_index = pd.read_csv(self.file_3,
                                       skipinitialspace=True)  # skip whitespace in header
            df_db2_column = pd.read_csv(self.file_4,
                                        skipinitialspace=True).dropna()  # skip whitespace in header
            df_db2_foreign_key = pd.read_csv(self.file_5,
                                             skipinitialspace=True)  # skip whitespace in header
            for index, table in df_db2_table_details.iterrows():
                table_name = table['TABLE_NAME']
                schema_name = table['SCHEMA_NAME']
                unique_dict = {}  # [{"key1":[],"key2":[]}]
                for count, uq in enumerate(df_db2_index[(df_db2_index['TABLE_NAME'] == table_name) & (
                        df_db2_index['SCHEMA_NAME'] == schema_name)]['UNIQUENESS'].drop_duplicates()):
                    unique_dict.update({'key' + str(count + 1): [uq]})
                index_dict = {}  # {"index1": [], "index2": []}
                for count, ind in enumerate(df_db2_index[(df_db2_index['TABLE_NAME'] == table_name) & (
                        df_db2_index['SCHEMA_NAME'] == schema_name)]['COLUMN_NAME']):
                    index_dict.update({"index" + str(count + 1): [ind]})
                columns = []
                col_name = df_db2_column[
                    (df_db2_column['TABLE_NAME'] == table_name) & (df_db2_column['SCHEMA_NAME'] == schema_name)]
                for count, col in col_name.iterrows():
                    columns.append(
                        {col['COLUMN_NAME']: {"column_order": 1,
                                              "data-type": col['DATA_TYPE'],
                                              "length": col['DATA_LENGTH'],
                                              "precision": '',
                                              "scale": '',
                                              "min_value": '',
                                              "max_value": '',
                                              "avg_value": '',
                                              "min_length": '',
                                              "max_length": '',
                                              "avg_length": '',
                                              "decimal_value": '',
                                              "count": "",
                                              "count_distinct": '',
                                              "nullable": ''
                                              }})
                entity_relation = []
                foreign_key = df_db2_foreign_key[(df_db2_foreign_key['TABLE_NAME'] == table_name) & (
                            df_db2_foreign_key['SCHEMA_NAME'] == schema_name)]
                for count, foreign in foreign_key.iterrows():
                    entity_relation.append({"reference_entity": '',
                                            "target_cols": foreign['COLUMN_NAME'],
                                            "target_table": foreign['TABLE_NAME'],
                                            "target_type": "F",
                                            "source_cols": foreign['PARENT_COLUMN_NAME'],
                                            "source_table": foreign['PARENT_TABLE_NAME'],
                                            "source_type": "P",
                                            "relationship_type": "1-M" if foreign[
                                                                              'JOIN_TYPE'] == 'Referential' else "1-1"})
                json_dict.update(
                    {"entity_" + table_name: {
                        "entity_key": str(random.randint(0, 100000)) + datetime.now().strftime("%d%m%Y%H%M%S"),
                        "db_type": "Oracle",
                        "row_count": table['NUMBER_OF_ROWS'],
                        "schema_name": table['SCHEMA_NAME'],
                        "object_name": table_name,
                        "object_type": 'Table',
                        "primary_key": df_db2_primary_key[(df_db2_primary_key['TABLE_NAME'] == table_name) & (
                                    df_db2_primary_key['SCHEMA_NAME'] == schema_name)]['COLUMN_NAME'].tolist(),
                        "unique_key": [unique_dict],
                        "indexes": [index_dict],
                        "columns": columns,
                        "entity_relation": entity_relation}})
            json.dump(json_dict, self.file_object, indent=4)  # indent with 4 tabs
        except KeyError as ke:
            print("Column %s does not exist" % ke.args[0])
        except FileNotFoundError as fnf:
            print(fnf.strerror + ': ' + fnf.filename)
        except Exception as e:
            print("--in Exception---", e)
        finally:
            print("The 'try except' is finished")
            f_size = self.file_object.tell()  # get file size in bytes before closing the file
            self.file_object.close()
            if not f_size:
                os.remove(self.file_object.name)  # removing the empty file to save disk space
        return True

# db2_analyzer()
if __name__ == "__main__":
    db2_obj = Db2Analyzer()
    db2_obj.download_files()
