from typing import List
from pyspark.sql.types import (TimestampType, 
            StringType, StructType, StructField, BooleanType, IntegerType)
from claims.exception import ClaimsException
from pyspark.sql import DataFrame
import os, sys
from typing import Dict

class ClaimsDataSchema:

    def __init__(self):
        self.Label1: str = 'Label1'
        self.Label2: str = 'Label2'
        self.DINLevel1ClassCode = 'DINLevel1ClassCode'
        self.ExpenseType: str = 'ExpenseType'
        self.ReceivedDate: str = 'ReceivedDate'
        self.PaymentMonth: str = 'PaymentMonth'
        self.MemberIDscrambled: str = 'MemberIDscrambled'
        self.ClaimSubmissionChannel: str = 'ClaimSubmissionChannel'
        self.ClaimantAge: str = 'ClaimantAge'
        self.ClaimantGender: str = 'ClaimantGender'
        self.FacilityIDscrambled: str = 'FacilityIDscrambled'
        self.MemberCity: str = 'MemberCity'
        self.MemberProvince: str = "MemberProvince"
        self.PaymentIssueDate: str = "PaymentIssueDate"
        self.ServiceDate: str = "ServiceDate"
        self.SubmittedAmount: str = "SubmittedAmount"
        self.UniqueClaimCount: str = "UniqueClaimCount"


    @property
    def dataframe_schema(self) -> StructType:
        try:
            schema = StructType([
                StructField(self.Label1, StringType()),
                StructField(self.Label2, StringType()),
                StructField(self.DINLevel1ClassCode, StringType()),
                StructField(self.ExpenseType, StringType()),
                StructField(self.ReceivedDate, StringType()),
                StructField(self.MemberIDscrambled, StringType()),
                StructField(self.ClaimSubmissionChannel, StringType()),
                StructField(self.ClaimantAge, StringType()),
                StructField(self.ClaimantGender, StringType()),
                StructField(self.FacilityIDscrambled, StringType()),
                StructField(self.MemberCity, StringType()),
                StructField(self.MemberProvince, StringType()),
                StructField(self.PaymentIssueDate, StringType()),
                StructField(self.ServiceDate, StringType()),
                StructField(self.SubmittedAmount, StringType()),
                StructField(self.UniqueClaimCount, StringType()),

            ])
            return schema

        except Exception as e:
            raise ClaimsException(e, sys) from e

    @property
    def target_column(self):
        return  [self.Label1 , self.Label2]


    @property
    def one_hot_encoding_features(self) -> List[str]:
        features = [
            self.DINLevel1ClassCode,
            self.ClaimSubmissionChannel,
            self.ClaimantGender,
            self.MemberProvince,
        ]
        return features

    @property
    def im_one_hot_encoding_features(self) -> List[str]:
        return [f"im_{col}" for col in self.one_hot_encoding_features]


    @property
    def string_indexer_one_hot_features(self) -> List[str]:
        return [f"si_{col}" for col in self.one_hot_encoding_features]

    @property
    def tf_one_hot_encoding_features(self) -> List[str]:
        return [f"tf_{col}" for col in self.one_hot_encoding_features]


    # @property
    # def derived_input_features(self) -> List[str]:
    #     features = [
    #         self.PaymentIssueDate,
    #         self.ReceivedDate
    #     ]
    #     return features

    # @property
    # def derived_output_features(self) -> List[str]:
    #     return [self.col_diff_in_days]


    # @property
    # def numerical_columns(self) -> List[str]:
    #     return self.derived_output_features

    # @property
    # def im_numerical_columns(self) -> List[str]:
    #     return [f"im_{col}" for col in self.numerical_columns]



    # @property
    # def tfidf_features(self) -> List[str]:
    #     features = [
    #         self.col_issue
    #     ]
    #     return features

    # @property
    # def tf_tfidf_features(self) -> List[str]:
    #     return [f"tf_{col}" for col in self.tfidf_features]

    @property
    def input_features(self) -> List[str]:
        in_features = self.tf_one_hot_encoding_features # + self.im_numerical_columns
        return in_features

    # @property
    # def required_prediction_columns(self) -> List[str]:
    #     features =  self.one_hot_encoding_features + self.tfidf_features + \
    #                [self.col_date_sent_to_company, self.col_date_received]
    #     return features

    #(+ self.tfidf_features) + [self.col_date_sent_to_company, self.col_date_received]
    @property
    def required_columns(self) -> List[str]:
        features = self.target_column + self.one_hot_encoding_features + \
        [self.ReceivedDate, self.MemberIDscrambled, self.ClaimantAge, self.FacilityIDscrambled, self.MemberCity, self.PaymentIssueDate, self.ServiceDate, self.SubmittedAmount]

        return features

    @property
    def unwanted_columns(self) -> List[str]:
        features = [
            self.ExpenseType,
            self.UniqueClaimCount]

        return features

    @property
    def vector_assembler_output(self) -> str:
        return "va_input_features"


    @property
    def scaled_vector_input_features(self) -> str:
        return "scaled_input_features"


    @property
    def target_indexed_label(self) -> str:
        return f"indexed_{self.target_column}"

    @property
    def prediction_column_name(self) -> str:
        return "prediction"

    # @property
    # def prediction_label_column_name(self) -> str:
    #     return f"{self.prediction_column_name}_{self.target_column}"
