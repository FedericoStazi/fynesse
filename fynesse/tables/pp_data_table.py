import os
from urllib import request


class PPDataTable:
    def __int__(self, connection):
        self.connection = connection

    def create_table(self):
        cur = self.connection.cursor()
        cur.execute(f"""
            DROP TABLE IF EXISTS `pp_data`;
            CREATE TABLE IF NOT EXISTS `pp_data` (
                `transaction_unique_identifier` tinytext COLLATE utf8_bin NOT NULL,
                `price` int(10) unsigned NOT NULL,
                `date_of_transfer` date NOT NULL,
                `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
                `property_type` varchar(1) COLLATE utf8_bin NOT NULL,
                `new_build_flag` varchar(1) COLLATE utf8_bin NOT NULL,
                `tenure_type` varchar(1) COLLATE utf8_bin NOT NULL,
                `primary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
                `secondary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
                `street` tinytext COLLATE utf8_bin NOT NULL,
                `locality` tinytext COLLATE utf8_bin NOT NULL,
                `town_city` tinytext COLLATE utf8_bin NOT NULL,
                `district` tinytext COLLATE utf8_bin NOT NULL,
                `county` tinytext COLLATE utf8_bin NOT NULL,
                `ppd_category_type` varchar(2) COLLATE utf8_bin NOT NULL,
                `record_status` varchar(2) COLLATE utf8_bin NOT NULL,
                `db_id` bigint(20) unsigned NOT NULL
            ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1;
        """)

        rows = cur.fetchall()
        return rows

    def create_indices(self):
        cur = self.connection.cursor()
        cur.execute(f"""
            ALTER TABLE `pp_data`
            ADD PRIMARY KEY (`db_id`),
            MODIFY `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=1;
            CREATE INDEX `pp.postcode` USING HASH
                ON `pp_data`
                    (postcode);
            CREATE INDEX `pp.date` USING HASH
                ON `pp_data` 
                    (date_of_transfer);
        """)

        rows = cur.fetchall()
        return rows

    def load_data(self, *, start_year=1995, end_year=2021):
        cur = self.connection.cursor()
        for year in range(start_year, end_year + 1):
            for part in range(1, 3):
                print(f"\rLoading year {year} part {part}...", end="")

                filename = f"pp-{year}-part{part}.csv"
                url = f"http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/{filename}"
                request.urlretrieve(url, filename)

                cur.execute(f"""
                        LOAD DATA LOCAL INFILE '{filename}' INTO TABLE pp_data
                            FIELDS TERMINATED BY ','
                            LINES STARTING BY '' TERMINATED BY '\\n';
                    """)

                os.remove(filename)

        rows = cur.fetchall()
        return rows
