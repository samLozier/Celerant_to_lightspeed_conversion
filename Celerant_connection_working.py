import pyodbc

import pandas as pd

import config as cfg

driver = cfg.server['driver']
server = cfg.server['server']
database = cfg.server['database']
uid = cfg.server['uid']
pwd = cfg.server['pwd']

con_string = f'DRIVER={driver};SERVER={server};UID={uid};PWD={pwd};DATABASE={database};'
print(con_string)


def run_get_styles():
    cnxn = pyodbc.connect(con_string)
    print('connected')
    sql = """set nocount on
        select 
        TB_SKU_LOOKUPS.lookup as upc,
        TB_STYLES.STYLE AS STYLE,
        tb_styles.BRAND as brand, 
        tb_styles.DESCRIPTION as Description, 
        TB_ATTR1_ENTRIES.ATTR1 as Attr1,
        TB_ATTR2_ENTRIES.ATTR2 as Attr2, 
        TB_SIZE_ENTRIES.SIZ as Size,
        TB_SKU_BUCKETS.LAST_COST as cost, 
        TB_SKU_BUCKETS.SUGG_PRICE as Suggested, 
        TB_SKU_BUCKETS.PRICE as price, 
        TB_TAXONOMY.DEPT as dept, 
        TB_TAXONOMY.TYP as type, 
        tb_taxonomy.SUBTYP_1 as Sub1,
        tb_taxonomy.SUBTYP_2 as sub2,
        tb_taxonomy.SUBTYP_3 as sub3, 
        /* TB_TAX_CODES.TAX_CODE_LABEL, */
        CASE TB_TAX_CODES.TAX_CODE_LABEL
            WHEN null then 'No'
            else 'Yes' END as Taxable,
        /* TB_STYLES.NON_INVT as 'Item Type', */
        case TB_STYLES.NON_INVT 
            when 'N' then 'Single'
            else 'Non-inventory' END as 'Item Type',
        /* TB_STYLES.MUST_SERIAL, */
        case tb_styles.MUST_SERIAL
            when 'Y' then 'Yes'
            else 'NO' END as 'Serialized',
        TB_CONTACTS.COMPANY AS 'VENDOR',  
        TB_PARTS.PART_NUM AS 'VENDOR ID'
        from TB_SKU_LOOKUPS
        inner join TB_SKUS on TB_SKU_LOOKUPS.SKU_ID = TB_SKUS.SKU_ID
        inner join TB_STYLES on TB_SKUS.style_ID = tb_styles.STYLE_ID
        left join TB_ATTR1_ENTRIES on TB_SKUS.ATTR1_ENTRY_ID = TB_ATTR1_ENTRIES.ATTR1_ENTRY_ID
        left join TB_ATTR2_ENTRIES on TB_SKUS.ATTR2_ENTRY_ID = TB_ATTR2_ENTRIES.ATTR2_ENTRY_ID
        left join TB_SIZE_ENTRIES on TB_SKUS.SCALE_ENTRY_ID = TB_SIZE_ENTRIES.SCALE_ENTRY_ID
        inner join TB_SKU_BUCKETS on TB_SKUS.SKU_ID = TB_SKU_BUCKETS.SKU_ID
        inner join TB_TAXONOMY on TB_STYLES.TAXONOMY_ID = TB_TAXONOMY.TAXONOMY_ID
        LEFT join TB_TAX_CODES ON TB_STYLES.TAX_CODE_ID = TB_TAX_CODES.TAX_CODE_ID
        LEFT JOIN TB_PARTS ON TB_PARTS.STYLE_ID = TB_STYLES.STYLE_ID
        LEFT JOIN TB_CONTACTS ON TB_CONTACTS.CONTACT_ID = TB_PARTS.CONTACT_ID
        where TB_SKU_LOOKUPS.PRIME = 'Y'
        and 
        TB_SKU_BUCKETS.STORE_ID = '1'
        AND 
        (CONVERT(DATE,TB_STYLES.DLU) > CONVERT(DATE, GETDATE()-365) OR TB_SKU_BUCKETS.QOH != 0)
        ORDER BY
        TB_STYLES.STYLE,
        tb_sku_buckets.store_id
        ;"""
    print(sql)
    df_data = pd.read_sql(sql, cnxn)
    cnxn.close()

    # return df_data
    df_data.to_csv('styles_to_parse.csv', index=False)

run_get_styles()
