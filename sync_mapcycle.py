import os
import sys
import vdf
import pymysql
import argparse
from collections.abc import Mapping

GROUP_OPTIONS = [
    'display-template',
    'maps_invote',
    'group_weight',
    'next_mapgroup',
    'default_min_players',
    'default_max_players',
    'default_min_time',
    'default_max_time',
    'default_allow_every',
    'command',
    'nominate_flags',
    'adminmenu_flags',
]

parser = argparse.ArgumentParser(description="Process umc_mapcycle.txt and apply its mapcycle to umc-custom-weight plugin's database")

parser.add_argument('--mapcycle-path', help='Path to umc_mapcycle.txt', required=True)
parser.add_argument('--host', help='MYSQL host', required=True)
parser.add_argument('--user', help='MYSQL database user', required=True)
parser.add_argument('--password', help='MYSQL database password', required=True)
parser.add_argument('--database', help='MYSQL database name', required=True)

args = parser.parse_args()

MAPCYCLE_PATH = args.mapcycle_path

SQL_CREDENTIAL = {
    'host': args.host,
    'user': args.user,
    'password': args.password,
    'database': args.database,
    'charset': 'utf8mb4'
}

if not os.path.isfile(MAPCYCLE_PATH):
    print('Invalid mapcycle path', file=sys.stderr)
    sys.exit(1)

with open(MAPCYCLE_PATH, 'r', encoding='UTF-8') as mapCycleFile:
    try:
        mapCycle = vdf.load(mapCycleFile)
    except SyntaxError as e:
        print('vdf file error occured', e, file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print('Error occured', e, file=sys.stderr)
        sys.exit(1)

mapCycleList = []

for groupKey, groupValue in mapCycle['umc_mapcycle'].items():
    if not isinstance(groupValue, Mapping):
        continue

    for key in groupValue:
        if key in GROUP_OPTIONS:
            continue
        
        mapCycleList.append((groupKey, key))

with pymysql.connect(**SQL_CREDENTIAL) as connection:
    with connection.cursor() as cursor:
        statement = '''CREATE TEMPORARY TABLE `temp_weight` (
	                       `MapName` VARCHAR(256) NOT NULL,
	                       `GroupName` VARCHAR(256) NOT NULL,
	                       PRIMARY KEY (`MapName`, `GroupName`)
                       );'''
        cursor.execute(statement)
    
    with connection.cursor() as cursor:
        statement = 'INSERT IGNORE INTO `temp_weight` (`GroupName`, `MapName`) VALUES (%s, %s);'
        cursor.executemany(statement, mapCycleList)
    
    with connection.cursor() as cursor:
        statement = '''DELETE `umc` FROM `umc_map_weight` as `umc`
                           LEFT JOIN `temp_weight` as `temp`
                           ON `umc`.`MapName` = `temp`.`MapName` AND `umc`.`GroupName` = `temp`.`GroupName`
                           WHERE `temp`.`MapName` IS NULL OR `temp`.`GroupName` IS NULL;'''
        cursor.execute(statement)
    
    connection.commit()