from statistics import median
from flask import Flask, render_template, request
from sqlalchemy import create_engine
import pandas as pd

app = Flask(__name__)

conn = create_engine("sqlite:///Fission.db", echo=False)

# pd.set_option('display.width', 1000)
pd.set_option('colheader_justify', 'center')

property_df = pd.read_sql_table('property_info', conn)  
price_df = pd.read_sql_table('price', conn)
geo_df = pd.read_sql_table('geography', conn)
city_df = pd.read_sql_table('city', conn)
zipcode_df = pd.read_sql_table('zipcode', conn)
hometype_df = pd.read_sql_table('home_type', conn)
county_df = pd.read_sql_table('county', conn)


@app.route('/', methods=['GET', 'POST'])
def home():
    if request.method == 'POST':
        try:
            zipcode = request.form.get('zipcode')
            # df = pd.read_sql_query(query, conn)
            if len(zipcode) == 5:
                zip_df = zipcode_df.loc[zipcode_df['zipcode'] == int(zipcode)]
                # print(zip_df)
                # print(geo_df.head(5))
                geo_merged = geo_df.merge(zip_df, on='zip_index')\
                    .merge(city_df, on='city_index')\
                        .merge(county_df, how='left', on='county_index')
                # print(geo_merged.info())
                geo_merged = geo_merged[['geo_index', 
                    'zipcode', 'city', 'county'
                    ]]
                # print(county_df)
                property_merged = property_df.merge(price_df, on='price_index')\
                    .merge(geo_merged, on='geo_index')\
                        .merge(hometype_df, on='hometype_index')
                
                property_merged = property_merged.drop([
                    'geo_index', 'price_index', 'hometype_index'
                ], axis =1 )

                max_price = property_merged['price'].max()
                min_price = property_merged['price'].min()
                mean_price = property_merged['price'].mean()
                median_price = property_merged['price'].median()
                std_price = property_merged['price'].std()

                return render_template('index.html', 
                tables=[property_merged.to_html(classes="data", col_space=300, index=False)], 
                titles=[''],
                max_val = '$' + str(round(max_price, 2)),
                min_val = '$' + str(round(min_price, 2)),
                mean_val = '$' + str(round(mean_price, 2)),
                median_val = '$' + str(round(median_price, 2)),
                std_val = '$' + str(round(std_price, 2))
                )
            else:
                return render_template('index.html')
        except:
            return render_template('index.html')
    else:
        return render_template('index.html')



if __name__ == "__main__":
    app.run(host="localhost", port=5000)
