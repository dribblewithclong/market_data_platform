import re
import os
import json
import time
import datetime
import pytz
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from pyspark import SparkConf
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import types


class AsinInfoIngest:
    def __init__(
        self,
        rundate_path: str,
        num_partition: int = 24,
        sample_files: int = None,
    ) -> None:
        load_dotenv(
            re.search(
                f'.*{re.escape("market_data_platform")}',
                __file__,
            ).group() + '/.env'
        )

        self.rundate_path = rundate_path
        self.num_partition = num_partition
        self.sample_files = sample_files
        self.raw_dir = (
            f'bronze/amazon/asin_info/raw'
            f'/{self.rundate_path}/*.html'
        )
        self.minio_lakehouse = 's3a://lakehouse/'

    @staticmethod
    def parse_html(file):
        path, content = file
        soup = BeautifulSoup(
            content,
            "html.parser",
        )

        data = dict()

        data['asin'] = path.split(
            '/'
        )[-1].replace(
            '.html',
            '',
        )
        data['country'] = 'USA'

        product_name_el = soup.find(
            name='span',
            attrs={
                'id': 'productTitle',
            }
        )
        if product_name_el:
            data['product_name'] = product_name_el.text.strip()
        else:
            data['product_name'] = soup.find(
                name='h1',
                attrs={
                    'id': 'title',
                }
            ).text.strip()

        brand_el = soup.find(
            name='tr',
            attrs={
                'class': 'a-spacing-small po-brand',
            }
        )
        if brand_el:
            data['brand'] = brand_el.text.replace(
                'Brand',
                '',
            ).strip()
        else:
            brand_el = soup.find(
                name='a',
                attrs={
                    'id': 'bylineInfo',
                }
            )
            if brand_el:
                data['brand'] = brand_el.text.replace(
                    'Brand:',
                    '',
                ).replace(
                    'Visit the',
                    '',
                ).replace(
                    'Store',
                    '',
                ).strip()
            else:
                data['brand'] = 'Unknown'

        product_description_el = soup.find(
            name='div',
            attrs={
                'id': 'feature-bullets',
            }
        )
        if product_description_el:
            product_description_el = product_description_el.find(
                name='ul',
                attrs={
                    'class': 'a-unordered-list a-vertical a-spacing-mini',
                }
            )
            if product_description_el:
                product_description_el = product_description_el.find_all(
                    name='li',
                    attrs={
                        'class': 'a-spacing-mini',
                    }
                )
                product_description = ' | '.join(
                    [
                        i.text.strip()
                        for i in product_description_el
                    ]
                )
                data['product_description'] = product_description
            else:
                data['product_description'] = None
        else:
            product_description_el = soup.find(
                name='div',
                attrs={
                    'id': 'productFactsDesktop_feature_div',
                }
            )
            if product_description_el:
                data['product_description'] = \
                    product_description_el.text.strip()
            else:
                data['product_description'] = None

        product_attrs = {}
        product_attrs_el = soup.find(
            name='div',
            attrs={
                'id': 'productOverview_feature_div',
            }
        )
        if product_attrs_el:
            product_attrs_el = product_attrs_el.find(
                name='table',
                attrs={
                    'class': 'a-normal a-spacing-micro',
                }
            )
            if product_attrs_el:
                product_attrs_el = product_attrs_el.find_all(
                    name='tr',
                )
                for i in product_attrs_el:
                    try:
                        product_attrs[
                            i['class'][-1].replace(
                                'po-',
                                '',
                            )
                        ] = i.find(
                            name='span',
                            attrs={
                                'class': 'a-size-base po-break-word',
                            }
                        ).text.strip()
                    except Exception:
                        product_attrs[
                            i['class'][-1].replace(
                                'po-',
                                '',
                            )
                        ] = i.find(
                            name='span',
                            attrs={
                                'class': 'a-size-base',
                            }
                        ).text.strip()

        data['product_attribute'] = json.dumps(
            product_attrs
        )

        product_details = {}
        product_details_el = soup.find(
            name='table',
            attrs={
                'id': 'productDetails_techSpec_section_1',
            }
        )
        if product_details_el:
            for i in product_details_el.find_all(
                name='tr'
            ):
                product_details[
                    i.find(
                        name='th',
                    ).text.strip().lower().replace(
                        ' ',
                        '_',
                    )
                ] = i.find(
                    name='td',
                ).text.replace(
                    '\u200e',
                    '',
                ).strip()
        additional_details_el = soup.find(
            name='table',
            attrs={
                'id': 'productDetails_detailBullets_sections1',
            }
        )
        if additional_details_el:
            for i in additional_details_el.find_all(
                name='tr'
            ):
                product_details[
                    i.find(
                        name='th',
                    ).text.strip().lower().replace(
                        ' ',
                        '_',
                    )
                ] = i.find(
                    name='td',
                ).text.replace(
                    '\u200e',
                    '',
                ).strip()
        data['product_details'] = json.dumps(
            product_details
        )

        product_category_el = soup.find(
            name='div',
            attrs={
                'id': 'wayfinding-breadcrumbs_feature_div',
            }
        )
        if product_category_el:
            product_category = ' > '.join(
                [
                    i.text.strip()
                    for i in product_category_el.find_all(
                        name='li'
                    )
                    if not i.get('class')
                ]
            )
            data['product_category'] = product_category
        else:
            data['product_category'] = 'Unknown'

        price_el = soup.find(
            name='div',
            attrs={
                'id': 'corePriceDisplay_desktop_feature_div',
            }
        )
        if not price_el:
            data['price'] = None
            data['price_raw'] = None
        else:
            exact_price_el = price_el.find(
                name='span',
                attrs={
                    'class': 'a-price aok-align-center '
                    'reinventPricePriceToPayMargin priceToPay',
                }
            )
            if exact_price_el:
                data['price'] = float(
                    exact_price_el.text.strip().replace(
                        '$',
                        '',
                    ).replace(
                        ',',
                        '',
                    )
                )
                data['price_raw'] = price_el.text.strip()
            else:
                data['price'] = None
                data['price_raw'] = None

        rating_el = soup.find(
            name='div',
            attrs={
                'id': 'averageCustomerReviews_feature_div',
            }
        )
        if not rating_el:
            data['overall_rating'] = None
            data['overall_num_rating'] = 0
        else:
            rating_el = soup.find(
                name='div',
                attrs={
                    'id': 'averageCustomerReviews_feature_div',
                }
            ).find(
                name='div',
                attrs={
                    'id': 'averageCustomerReviews',
                }
            )
            if not rating_el:
                data['overall_rating'] = None
                data['overall_num_rating'] = 0
            else:
                overall_rating_el = rating_el.find(
                    name='span',
                    attrs={
                        'id': 'acrPopover',
                    }
                )
                if overall_rating_el:
                    data['overall_rating'] = float(
                        overall_rating_el.get(
                            'title'
                        ).split(
                            'out'
                        )[0].strip()
                    )
                else:
                    data['overall_rating'] = None
                overall_num_rating_el = rating_el.find(
                    name='span',
                    attrs={
                        'data-csa-c-func-deps': 'aui-da-acrLink-click-metrics',
                    }
                )
                if overall_num_rating_el:
                    data['overall_num_rating'] = int(
                        overall_num_rating_el.text.replace(
                            'ratings',
                            '',
                        ).replace(
                            'rating',
                            '',
                        ).strip().replace(
                            ',',
                            '',
                        )
                    )
                else:
                    data['overall_num_rating'] = None

        data['last_updated'] = datetime.datetime.now(
                pytz.timezone('Asia/Ho_Chi_Minh'),
            ).replace(
                tzinfo=None,
            )

        return data

    def spark_config(
        self,
        app_name: str = 'insideout',
    ) -> SparkSession:
        nessie_catalog_branch = 'main'
        jar_packages = [
            # MinIO
            'org.apache.hadoop:hadoop-aws:3.3.4',
            # Iceberg
            'org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.6.1',
        ]

        conf = (
            SparkConf().setAppName(app_name)
            # Jar package
            .set(
                'spark.jars.packages',
                ','.join(jar_packages),
            )
            # Iceberg Spark extension
            .set(
                'spark.sql.extensions',
                'org.apache.iceberg.spark.extensions.'
                'IcebergSparkSessionExtensions',
            )
            # Nessie catalog
            .set(
                'spark.sql.catalog.nessie.uri',
                os.getenv(
                    'NESSIE_CATALOG_URI'
                ),
            ).set(
                'spark.sql.catalog.nessie.ref',
                nessie_catalog_branch,
            ).set(
                'spark.sql.catalog.nessie.authentication.type',
                'NONE',
            ).set(
                'spark.sql.catalog.nessie.catalog-impl',
                'org.apache.iceberg.nessie.NessieCatalog',
            ).set(
                'spark.sql.catalog.nessie.warehouse',
                self.minio_lakehouse,
            ).set(
                'spark.sql.catalog.nessie',
                'org.apache.iceberg.spark.SparkCatalog'
            )
        )

        spark = SparkSession.builder.master(
            f'spark://{os.getenv("SPARK_HOST")}'
        ).config(
            conf=conf
        ).getOrCreate()

        # MinIO config
        sc = spark.sparkContext
        sc._jsc.hadoopConfiguration().set(
            "fs.s3a.access.key",
            os.getenv("MINIO_ACCESS_KEY"),
        )
        sc._jsc.hadoopConfiguration().set(
            "fs.s3a.secret.key",
            os.getenv("MINIO_SECRET_KEY"),
        )
        sc._jsc.hadoopConfiguration().set(
            "fs.s3a.endpoint",
            f"http://{os.getenv('MINIO_HOST')}",
        )
        sc._jsc.hadoopConfiguration().set(
            "fs.s3a.path.style.access",
            "true",
        )
        sc._jsc.hadoopConfiguration().set(
            "fs.s3a.connection.ssl.enabled",
            "false",
        )

        return spark

    def transform(
        self,
        spark: SparkSession,
    ) -> DataFrame:
        try:
            html_rdd = spark.sparkContext.wholeTextFiles(
                f"{self.minio_lakehouse}{self.raw_dir}"
            )
            if self.sample_files:
                repartition_html_rdd = spark.sparkContext.parallelize(
                    html_rdd.take(self.sample_files)
                ).repartition(self.num_partition)
            else:
                repartition_html_rdd = html_rdd.repartition(self.num_partition)

            # Process data files
            parsed_data = repartition_html_rdd.map(self.parse_html)

            # Define table schema
            schema = types.StructType(
                [
                    types.StructField(
                        "asin", types.StringType(), False
                    ),
                    types.StructField(
                        "country", types.StringType(), False
                    ),
                    types.StructField(
                        "product_name", types.StringType(), False
                    ),
                    types.StructField(
                        "brand", types.StringType(), True
                    ),
                    types.StructField(
                        "product_description", types.StringType(), True
                    ),
                    types.StructField(
                        "product_attribute", types.StringType(), True
                    ),
                    types.StructField(
                        "product_details", types.StringType(), True
                    ),
                    types.StructField(
                        "product_category", types.StringType(), True
                    ),
                    types.StructField(
                        "price", types.FloatType(), True
                    ),
                    types.StructField(
                        "price_raw", types.StringType(), True
                    ),
                    types.StructField(
                        "overall_rating", types.FloatType(), True
                    ),
                    types.StructField(
                        "overall_num_rating", types.IntegerType(), True
                    ),
                    types.StructField(
                        "last_updated", types.TimestampType(), False
                    ),
                ]
            )

            # Construct dataframe
            df = spark.createDataFrame(parsed_data, schema)

            return df
        except Exception:
            spark.stop()
            raise

    def main(
        self,
        app_name: str = 'insideout',
    ) -> None:
        spark = self.spark_config(app_name)

        # Transform data
        df = self.transform(spark)

        des_table = 'silver.asin_info'
        stg_table = 'asin_info_stag_iykyk'
        df.createOrReplaceTempView(stg_table)

        # Upsert data
        try:
            spark.sql(
                f"""
                MERGE INTO
                    nessie.{des_table} sink
                USING
                    {stg_table} source
                ON
                    sink.asin = source.asin
                    AND sink.country = source.country
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
                """
            )
        except Exception:
            spark.stop()
            raise

        spark.stop()


if __name__ == '__main__':
    start = time.time()

    rundate_path = datetime.datetime.now(
        pytz.timezone('Asia/Ho_Chi_Minh')
    ).strftime(
        '%Y/%m/%d'
    )

    j = AsinInfoIngest(
        rundate_path=rundate_path,
        num_partition=2400,
    )
    j.main(
        f'transform_asin_info_{rundate_path}',
    )

    end = time.time()
    print(f'Total time {end - start}')
