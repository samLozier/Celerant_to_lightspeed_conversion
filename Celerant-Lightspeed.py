import pandas as pd
from tqdm import tqdm

df_styles = pd.read_csv('styles_to_parse.csv', na_values=dict.fromkeys(['Attr1', 'Attr2', 'Size'], ['-', '_']))
df_styles['upc'] = df_styles['upc'].str.strip()


def check_upc(lookup):
    if len(lookup) == 12 and lookup.isdigit():
        lookup_type = 'UPC'
    elif len(lookup) == 13 and lookup.isdigit():
        lookup_type = 'EAN'
    else:
        lookup_type = 'Custom SKU'
    return lookup_type, lookup


def attributes(rows):
    attrs = {key: (f'Attr{i + 1}', val) for i, (key, val) in enumerate(rows.items()) if not pd.isnull(val)}

    return {'Attributeset': '/'.join(attrs.keys()), **dict(attrs.values())}


def check_variables(matrix_style):  # get input df_view df_this_style['attr1', 'attr3', 'size]
    keep = ['upc', 'STYLE', 'brand', 'Description', 'cost', 'Suggested', 'price', 'dept', 'type', 'Sub1', 'sub2',
            'sub3', 'Taxable', 'Item Type', 'Serialized', 'VENDOR', 'VENDOR ID']
    matrix_style_output = matrix_style[keep].join(
        pd.DataFrame(matrix_style.drop(columns=keep).apply(attributes, axis=1).tolist()))

    attr_cols = [col for col in matrix_style_output.columns if 'Attr' in col and len(col) < 6]
    col_count = len(attr_cols)
    newcols = []
    for i in range(col_count):
        newnum = i + 1
        newattr = attr_cols[i].replace(attr_cols[i], f'Attr{newnum}')
        newcols.append(newattr)

    for col in range(len(attr_cols)):
        matrix_style_output.rename(columns={attr_cols[col]: newcols[col]}, inplace=True)
    return matrix_style_output


# get list of unique styles - these don't have variants
df_unique_styles = df_styles['STYLE'].unique().tolist()
starting_length = len(df_styles)  # for error checking at the end (making sure I didn't accidentally filter data)
starting_unique_count = len(df_unique_styles)  # also for error checking at the end.
output = []  # to be used for creating the final export dataframe

for unique_style in tqdm(df_unique_styles):  # find unique styles from the full dataset
    df_this_style = df_styles.loc[df_styles['STYLE'] == unique_style]  # return a df for each set of skus in a style

    if len(df_this_style) == 1:  # find styles with only one row
        if df_this_style[['Attr1', 'Attr2', 'Size']].isna().values.all():
            lookup_type, lookup = check_upc(df_this_style['upc'].values[0])  # single line, so series access, not key

            if df_this_style['Taxable'].values[0] == 'Yes':
                tax_class = 'Item'
            else:
                tax_class = 'Food and Permits'
                # commented dictionary keys in newline are for fields I haven't pulled in the query yet.

            if df_this_style['VENDOR'].values[0] is not None:
                vendor = df_this_style['VENDOR'].values[0]
            else:
                vendor = ''

            if df_this_style['VENDOR ID'].values[0] is not None:
                vendor_id = df_this_style['VENDOR ID'].values[0]
            else:
                vendor_id = ''

            if df_this_style['Serialized'].values[0] is not None:
                serialized = 'Yes'
            else:
                serialized = 'No'

            newline = {
                lookup_type: lookup,
                'Manufacturer SKU': df_this_style['STYLE'].values[0],
                'Description': df_this_style['Description'].values[0],  # use "matrix description for Matrix"
                'Quantity On Hand': 0,
                'MSRP - Price': df_this_style['Suggested'].values[0],
                'Default Cost': df_this_style['cost'].values[0],
                'Default - Price': df_this_style['price'].values[0],
                'Taxable': df_this_style['Taxable'].values[0],
                'Tax Class': tax_class,
                'Item Type': df_this_style['Item Type'].values[0],
                'Category': df_this_style['dept'].values[0],
                'Sub Category': df_this_style['type'].values[0],
                'Sub Category 2': df_this_style['Sub1'].values[0],
                'Sub Category 3': df_this_style['sub2'].values[0],
                'Sub Category 4': df_this_style['sub3'].values[0],
                'Clear Existing Tags': 'No',
                'Add Tags': '',
                'Note': '',
                'Display Note': '',
                'Archive': '',
                'Featured Image': '',
                'Image': '',
                'Vendor': vendor,
                'Vendor ID/ Part Number': vendor_id,
                'Non Inventory Item': df_this_style['Item Type'].values[0],
                'Manufacturer': df_this_style['brand'].values[0],
                'Serialized Item': serialized,
                'Discountable': 'Yes',
                'Add Tags': ''
            }
            output.append(newline)

    else:  # has more than one row
        df_this_style.reset_index(drop=True, inplace=True)
        # reformat the column matrix information
        df_new_this_style = check_variables(df_this_style)

        for i in range(len(df_new_this_style)):
            lookup_type, lookup = check_upc(str(df_new_this_style['upc'].values[i]))

            try:
                attr1 = df_new_this_style['Attr1'].values[i]
            except:
                attr1 = ''
            try:
                attr2 = df_new_this_style['Attr2'].values[i]
            except:
                attr2 = ''
            try:
                attr3 = df_new_this_style['Attr3'].values[i]
            except:
                attr3 = ''

            if df_this_style['Taxable'].values[i] == 'Yes':
                tax_class = 'Item'
            else:
                tax_class = 'Food and Permits'
                # commented dictionary keys in newline are for fields I haven't pulled in the query yet.

            if df_this_style['VENDOR'].values[i] is not None:
                vendor = df_this_style['VENDOR'].values[i]
            else:
                vendor = ''

            if df_this_style['VENDOR ID'].values[i] is not None:
                vendor_id = df_this_style['VENDOR ID'].values[i]
            else:
                vendor_id = ''

            if df_this_style['Serialized'].values[i] is not None:
                serialized = 'Yes'
            else:
                serialized = 'No'

            newline = {
                lookup_type: lookup,
                'Manufacturer SKU': df_new_this_style['STYLE'].values[i],
                'Matrix Description': df_new_this_style['Description'].values[i],  # use "matrix description for Matrix"
                'Quantity On Hand': 0,
                'MSRP - Price': df_new_this_style['Suggested'].values[i],
                'Default Cost': df_new_this_style['cost'].values[i],
                'Default - Price': df_new_this_style['price'].values[i],
                'Taxable': df_this_style['Taxable'].values[i],
                'Tax Class': tax_class,
                'Item Type': df_this_style['Item Type'].values[i],
                'Clear Existing Tags': 'No',
                'Matrix Attribute Set': df_new_this_style['Attributeset'].values[i],
                'Attribute 1': attr1,
                'Attribute 2': attr2,
                'Attribute 3': attr3,
                'Category': df_new_this_style['dept'].values[i],
                'Sub Category': df_new_this_style['type'].values[i],
                'Sub Category 2': df_new_this_style['Sub1'].values[i],
                'Sub Category 3': df_new_this_style['sub2'].values[i],
                'Sub Category 4': df_new_this_style['sub3'].values[i],
                'Vendor': vendor,
                'Vendor ID/ Part Number': vendor_id,
                'Non Inventory Item': df_this_style['Item Type'].values[i],
                'Manufacturer': df_new_this_style['brand'].values[i],
                'Serialized Item': serialized,
                'Discountable': 'Yes',
                'Add Tags': ''
            }
            output.append(newline)

df_final_frame = pd.DataFrame(output)
df_final_frame = df_final_frame['Description', 'UPC', 'EAN', 'Custom SKU', 'Manufacturer SKU', 'Vendor', 'Vendor ID',
                                'Brand', 'Default Cost', 'Default - Price', 'MSRP - Price', 'Matrix Description',
                                'Matrix Attribute Set', 'Attribute 1', 'Attribute 2', 'Attribute 3', 'Discountable', 'Taxable',
                                'Tax Class', 'Item Type', 'Serialized', 'Category', 'Subcategory 1', 'Subcategory 2',
                                'Subcategory 3', 'Subcategory 4', 'Clear Existing Tags', 'Add Tags', 'Note',
                                'Display Note', 'Archive']
print('Starting Length: ', starting_length,
      'Starting Unique Count: ', starting_unique_count,
      'Ending Length: ', len(df_final_frame),
      'Ending Unique Count: ', df_final_frame['Manufacturer SKU'].unique().tolist()
      )
df_final_frame.to_csv('final_frame_test.csv', index=False)
