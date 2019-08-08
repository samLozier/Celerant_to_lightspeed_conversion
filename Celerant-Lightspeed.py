import pandas as pd

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
            'sub3', 'store']
    matrix_style_output = matrix_style[keep].join(
        pd.DataFrame(matrix_style.drop(columns=keep).apply(attributes, axis=1).tolist()))

    print(matrix_style_output)

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
df_starting_length = len(df_styles)  # for error checking at the end (making sure I didn't accidentally filter data)
df_starting_unique_count = len(df_unique_styles)  # also for error checking at the end.
output = []  # to be used for creating the final export dataframe

for unique_style in df_unique_styles:  # find unique styles from the full dataset
    df_this_style = df_styles.loc[df_styles['STYLE'] == unique_style]  # return a df for each set of skus in a style

    if len(df_this_style) == 1:  # find styles with only one row
        if df_this_style[['Attr1', 'Attr2', 'Size']].isna().values.all():
            lookup_type, lookup = check_upc(df_this_style['upc'].values[0])  # single line, so series access, not key

            # commented dictionary keys in newline are for fields I haven't pulled in the query yet.

            newline = {
                lookup_type: lookup,
                'Manufacturer SKU': df_this_style['STYLE'].values[0],
                'Description': df_this_style['Description'].values[0],  # use "matrix description for Matrix"
                'Quantity On Hand': 0,
                'MSRP': df_this_style['Suggested'].values[0],
                'Default Cost': df_this_style['cost'].values[0],
                'Price': df_this_style['price'].values[0],
                # 'Tax':
                # 'Tax Class':
                # 'Item Type':
                # 'Clear Existing Tags': 'No'
                'Category': df_this_style['dept'].values[0],
                'Sub Category': df_this_style['type'].values[0],
                'Sub Category 2': df_this_style['Sub1'].values[0],
                'Sub Category 3': df_this_style['sub2'].values[0],
                'Tags': df_this_style['sub3'].values[0],
                # 'Vendor':
                # 'Vendor ID/ Part Number':
                # 'Non Inventory Item': Yes/No
                'Manufacturer': df_this_style['brand'].values[0],
                # 'Serialized Item': yes/no
            }
            output.append(newline)
            # print(newline)

    else:  # has more than one row
        df_this_style.reset_index(drop=True, inplace=True)
        # reformat the column matrix information
        df_new_this_style = check_variables(df_this_style)
        # print(df_new_this_style)

        # now check the UPC format and assign to column
        # for row in df_new_this_style:

        for i in range(len(df_new_this_style)):
            print(i)
            lookup_type, lookup = check_upc(str(df_new_this_style['upc'].values[i]))

            # finally break it down by row and assign to the output list
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

            newline = {
                lookup_type: lookup,
                'Manufacturer SKU': df_new_this_style['STYLE'].values[i],
                'Matrix Description': df_new_this_style['Description'].values[i],  # use "matrix description for Matrix"
                'Quantity On Hand': 0,
                'MSRP': df_new_this_style['Suggested'].values[i],
                'Default Cost': df_new_this_style['cost'].values[i],
                'Price': df_new_this_style['price'].values[i],
                # 'Tax':
                # 'Tax Class':
                # 'Item Type':
                # 'Clear Existing Tags': 'No'
                'Matrix Attribute Set': df_new_this_style['Attributeset'].values[i],
                # 'Attribute 1': df_new_this_style['Attr1'].values[0],
                # 'Attribute 2': df_new_this_style['Attr2'].values[0],
                # 'Attribute 3': df_new_this_style['Attr3'].values[0],
                'Attribute 1': attr1,
                'Attribute 2': attr2,
                'Attribute 3': attr3,
                'Category': df_new_this_style['dept'].values[i],
                'Sub Category': df_new_this_style['type'].values[i],
                'Sub Category 2': df_new_this_style['Sub1'].values[i],
                'Sub Category 3': df_new_this_style['sub2'].values[i],
                'Tags': df_new_this_style['sub3'].values[i],
                # 'Vendor':
                # 'Vendor ID/ Part Number':
                # 'Non Inventory Item': Yes/No
                'Manufacturer': df_new_this_style['brand'].values[i],
                # 'Serialized Item': yes/no
            }
            # print(newline)
            output.append(newline)

df_final_frame = pd.DataFrame(output)
print(df_final_frame)
df_final_frame.to_csv('final_frame_test.csv', index=False)
