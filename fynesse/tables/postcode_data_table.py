import os
from urllib import request


class PostcodeDataTable:
    def __int__(self, connection):
        self.connection = connection

    def create_table(self, connection):
        cur = self.connection.cursor()
        cur.execute(f"""
            DROP TABLE IF EXISTS `postcode_data`;
            CREATE TABLE IF NOT EXISTS `postcode_data` (
            `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
            `status` enum('live','terminated') NOT NULL,
            `usertype` enum('small', 'large') NOT NULL,
            `easting` int unsigned,
            `northing` int unsigned,
            `positional_quality_indicator` int NOT NULL,
            `country` enum('England', 'Wales', 'Scotland', 'Northern Ireland', 'Channel Islands', 'Isle of Man') NOT NULL,
            `lattitude` decimal(11,8) NOT NULL,
            `longitude` decimal(10,8) NOT NULL,
            `postcode_no_space` tinytext COLLATE utf8_bin NOT NULL,
            `postcode_fixed_width_seven` varchar(7) COLLATE utf8_bin NOT NULL,
            `postcode_fixed_width_eight` varchar(8) COLLATE utf8_bin NOT NULL,
            `postcode_area` varchar(2) COLLATE utf8_bin NOT NULL,
            `postcode_district` varchar(4) COLLATE utf8_bin NOT NULL,
            `postcode_sector` varchar(6) COLLATE utf8_bin NOT NULL,
            `outcode` varchar(4) COLLATE utf8_bin NOT NULL,
            `incode` varchar(3)  COLLATE utf8_bin NOT NULL,
            `db_id` bigint(20) unsigned NOT NULL
            ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
        """)

        rows = cur.fetchall()
        return rows

    def create_indices(self, connection):
        cur = self.connection.cursor()
        cur.execute(f"""
            ALTER TABLE `postcode_data`
            DROP INDEX IF EXISTS `PRIMARY`,
            DROP INDEX IF EXISTS `po.postcode`,
            ADD PRIMARY KEY (`db_id`),
            MODIFY `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=1;
            CREATE INDEX `po.postcode` USING HASH
                ON `postcode_data`
                    (postcode);
        """)

        rows = cur.fetchall()
        return rows

    def load_data(self, connection):
        filename = "open_postcode_geo.csv.zip"
        url = "https://www.getthedata.com/downloads/open_postcode_geo.csv.zip"
        request.urlretrieve(url, filename)

        cur = self.connection.cursor()
        cur.execute("""
                ALTER TABLE `postcode_data`
                DROP INDEX IF EXISTS `PRIMARY`,
                DROP INDEX IF EXISTS `po.postcode`,
                ADD PRIMARY KEY (`db_id`),
                MODIFY `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=1;
                CREATE INDEX `po.postcode` USING HASH
                    ON `postcode_data`
                        (postcode);
            """)

        os.remove(filename)

        rows = cur.fetchall()
        return rows
