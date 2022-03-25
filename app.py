#from databricks import sql
#from datetime import date, datetime, timedelta
#import math
#import io
#import base64
import datetime
import json
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import collections

from flask import Flask, request, Response, json, jsonify, session, send_file, make_response
from flask_cors import CORS, cross_origin
from databricks import sql
from reliability.Distributions import Weibull_Distribution

app = Flask(__name__)
app.secret_key = "secret key"
#app.config['PERMANENT_SESSION_LIFETIME'] =  datetime.timedelta(minutes=10)

# SET CORS options on app configuration
app.config['CORS_ALLOW_HEADERS'] = "Content-Type"

CORS(app, support_credentials=True, resources={r"/*": {"origins": "*"}})

# GET /trackingList} : Get tracking details
@app.route("/trackingList", methods=['GET'])
@cross_origin(supports_credentials=True, methods=['GET'], allow_headers=['Content-Type', 'Access-Control-Allow-Origin','Authorization'])
def trackingList():

    connection = sql.connect(
        server_hostname='adb-226745366320813.13.azuredatabricks.net',
        http_path='/sql/1.0/endpoints/61b1bce9adcc6384',
        access_token='dapid4721dc6ca8b37c502db929a880cb2a1')

    query = "SELECT  Tracking.Parameter, Tracking.Point_Estimate, Tracking.Standard_Error, Tracking.Lower_CI, Tracking.Upper_CI, Tracking.group_key, Tracking.Date_of_computation, FailureGrouped.seag, FailureGrouped.mileage, FailureGrouped.total_cost, FailureGrouped.strata_unique_id, FailureGrouped.vehicle_production_date, FailureGrouped.vehi_engine_series, FailureGrouped.days_to_failure, FailureGrouped.month_to_failure, FailureGrouped.group_key AS f_group_key FROM dev_hvdb_dm_weibull_tool.hvtb_tg_weibull_test_tracking as Tracking, dev_hvdb_dm_weibull_tool.hvtb_tg_weibull_test_failure_grouped as FailureGrouped WHERE FailureGrouped.group_key=Tracking.group_key AND Tracking.Parameter IN ('Alpha', 'Beta', 'Gamma') ORDER BY Tracking.group_key;"
    df = pd.read_sql(query, connection)
    df['Parameter'] = df['Parameter'].astype('str')
    df['Point_Estimate'] = df['Point_Estimate'].astype('str')
    df['Standard_Error'] = df['Standard_Error'].astype('str')
    df['Lower_CI'] = df['Lower_CI'].astype('str')
    df['Upper_CI'] = df['Upper_CI'].astype('str')
    df['group_key'] = df['group_key'].astype('str')
    df['Date_of_computation'] = df['Date_of_computation'].astype('str')

    df['seag'] = df['seag'].astype('str')
    df['mileage'] = df['mileage'].astype('str')
    df['total_cost'] = df['total_cost'].astype('str')
    df['strata_unique_id'] = df['strata_unique_id'].astype('str')
    df['vehicle_production_date'] = df['vehicle_production_date'].astype('str')
    df['vehi_engine_series'] = df['vehi_engine_series'].astype('str')
    df['days_to_failure'] = df['days_to_failure'].astype('str')
    df['month_to_failure'] = df['month_to_failure'].astype('str')
    df['f_group_key'] = df['f_group_key'].astype('str')
    #print (df.head(5))
    return Response(json.dumps(df.to_dict('records'), sort_keys=False, indent=2, default=default), content_type='application/json; charset=utf-8')

#################################################################################

# GET /getGroupKey} : Get tracking details
@app.route("/getGroupKey", methods=['GET'])
@cross_origin(supports_credentials=True, methods=['GET'],allow_headers=['Content-Type', 'Access-Control-Allow-Origin','Authorization'])
def getGroupKey():
    connection = sql.connect(
        server_hostname='adb-226745366320813.13.azuredatabricks.net',
        http_path='/sql/1.0/endpoints/61b1bce9adcc6384',
        access_token='dapid4721dc6ca8b37c502db929a880cb2a1')

    query = "SELECT DISTINCT(group_key) AS GroupKey FROM dev_hvdb_dm_weibull_tool.hvtb_tg_weibull_test_tracking"
    df = pd.read_sql(query, connection)

    df['GroupKey'] = df['GroupKey'].astype('str')
    #df['GroupKey'] = df['GroupKey'].str[0:-14]
    return Response(json.dumps(df.to_dict('records'), sort_keys=False, indent=2, default=default), content_type='application/json; charset=utf-8')

#################################################################################

#01202_47_2018-08-01
# GET /cdfPlotByGroupID/{group_key} : Join and Get tracking and Failure Grouped details
@app.route("/cdfPlotByGroupID/<string:group_key>", methods=['GET'])
@cross_origin(supports_credentials=True, methods=['GET'], allow_headers=['Content-Type', 'Access-Control-Allow-Origin'])
def cdfPlotByGroupID(group_key):
    connection = sql.connect(
        server_hostname='adb-226745366320813.13.azuredatabricks.net',
        http_path='/sql/1.0/endpoints/61b1bce9adcc6384',
        access_token='dapid4721dc6ca8b37c502db929a880cb2a1')

    # try:
    # create cursor
    cursor = connection.cursor()

    #group_key = '07206_47_2017-01-01'
    group_key = group_key + '%'
    #SELECT Tracking.Parameter, Tracking.Point_Estimate, Tracking.group_key FROM dev_hvdb_dm_weibull_tool.hvtb_tg_weibull_test_tracking as Tracking WHERE Tracking.group_key LIKE %s AND Tracking.Parameter IN ('Alpha', 'Beta')
    query = "SELECT Tracking.Parameter, Tracking.Point_Estimate, Tracking.group_key FROM dev_hvdb_dm_weibull_tool.hvtb_tg_weibull_test_tracking as Tracking WHERE Tracking.group_key LIKE %s AND Tracking.Parameter IN ('Alpha', 'Beta')"
    cursor.execute(query, (group_key,))

    df = pd.DataFrame(cursor.fetchall(), columns = ['Parameter', 'Point_Estimate', 'group_key'])

    df1 = df['Point_Estimate'].apply(lambda x: '%.3f' % x).values.tolist()
    tempDF = pd.DataFrame({'Point_Estimate_new': df1})
    tempDF.rename(columns={'Point_Estimate_new': 'Point_Estimate'}, inplace=True)

    dropDF = df.drop(['Point_Estimate'], axis=1)
    finalDF = pd.concat([dropDF, tempDF], axis=1, join='inner')

    Alpha = finalDF['Point_Estimate'][0]
    Beta = finalDF['Point_Estimate'][1]

    dist = Weibull_Distribution(Alpha, Beta)

    y_axis_values = dist.CDF(show_plot=False)
    x_axis_values = [i * 130 for i in y_axis_values]

    dist.CDF(xvals=x_axis_values, xmin=1, xmax=121, show_plot=True)

    intDictionary = [{'Alpha': Alpha, 'Beta': Beta, 'X': x_axis_values, 'Y': y_axis_values}]
    df_final = pd.DataFrame(intDictionary)

    return (jsonify(df_final.to_json(orient='records')))

#################################################################################

# GET /pdfPlotByGroupID/{group_key} : Join and Get tracking and Failure Grouped details
@app.route("/pdfPlotByGroupID/<string:group_key>", methods=['GET'])
@cross_origin(supports_credentials=True, methods=['GET'], allow_headers=['Content-Type', 'Access-Control-Allow-Origin'])
def pdfPlotByGroupID(group_key):

    connection = sql.connect(
        server_hostname='adb-226745366320813.13.azuredatabricks.net',
        http_path='/sql/1.0/endpoints/61b1bce9adcc6384',
        access_token='dapid4721dc6ca8b37c502db929a880cb2a1')

    # create cursor
    cursor = connection.cursor()

    group_key = group_key + '%'
    query = "SELECT Tracking.Parameter, Tracking.Point_Estimate, Tracking.group_key FROM dev_hvdb_dm_weibull_tool.hvtb_tg_weibull_test_tracking as Tracking WHERE Tracking.group_key LIKE %s AND Tracking.Parameter IN ('Alpha', 'Beta')"
    cursor.execute(query, (group_key,))

    df = pd.DataFrame(cursor.fetchall(), columns = ['Parameter', 'Point_Estimate', 'group_key'])

    df1 = df['Point_Estimate'].apply(lambda x: '%.3f' % x).values.tolist()
    tempDF = pd.DataFrame({'Point_Estimate_new': df1})
    tempDF.rename(columns={'Point_Estimate_new': 'Point_Estimate'}, inplace=True)

    dropDF = df.drop(['Point_Estimate'], axis=1)
    finalDF = pd.concat([dropDF, tempDF], axis=1, join='inner')

    Alpha = finalDF['Point_Estimate'][0]
    Beta = finalDF['Point_Estimate'][1]

    dist = Weibull_Distribution(Alpha, Beta)

    #
    # y_axis_values = dist.PDF(show_plot=False)
    # y_axis_values_new = y_axis_values[:90]
    # x_axis_values = [i * 30000 for i in y_axis_values_new]
    #

    y_axis_values = dist.PDF(show_plot=False)
    y_axis_values_new = y_axis_values[:90]
    x_axis_values = [i * 30000 for i in y_axis_values_new]

    dist.PDF(xvals=x_axis_values, xmin=1, xmax=121, show_plot=True)

    intDictionary = [{'Alpha': Alpha, 'Beta': Beta, 'X': x_axis_values, 'Y': y_axis_values}]
    df_final = pd.DataFrame(intDictionary)
    return (jsonify(df_final.to_json(orient='records')))

#################################################################################

# GET /histogramPlotByGroupID/{group_key} : Join and Get tracking and Failure Grouped details
@app.route("/histogramPlotByGroupID/<string:group_key>", methods=['GET'])
@cross_origin(supports_credentials=True, methods=['GET'], allow_headers=['Content-Type', 'Access-Control-Allow-Origin'])
def histogramPlotByGroupID(group_key):
    connection = sql.connect(
        server_hostname='adb-226745366320813.13.azuredatabricks.net',
        http_path='/sql/1.0/endpoints/61b1bce9adcc6384',
        access_token='dapid4721dc6ca8b37c502db929a880cb2a1')

    # create cursor
    cursor = connection.cursor()

    group_key = group_key + '%'
    query = "SELECT FailureGrouped.month_to_failure FROM dev_hvdb_dm_weibull_tool.hvtb_tg_weibull_test_tracking as Tracking, dev_hvdb_dm_weibull_tool.hvtb_tg_weibull_test_failure_grouped as FailureGrouped WHERE Tracking.group_key LIKE %s AND FailureGrouped.group_key=Tracking.group_key"
    cursor.execute(query,(group_key,))

    df = pd.DataFrame(cursor.fetchall(), columns = ['month_to_failure'])

    return Response(json.dumps(df.to_dict('records'), sort_keys=False, indent=2, default=default), content_type='application/json; charset=utf-8')

#################################################################################

# GET /trackingListByGroupID/{group_key} : Get tracking details
@app.route("/trackingListByGroupID/<string:group_key>", methods=['GET'])
@cross_origin(supports_credentials=True, methods=['GET'], allow_headers=['Content-Type', 'Access-Control-Allow-Origin'])
def trackingListByGroupID(group_key):
    #params = {'group_key': group_key}
    #group_key = request.args.get('group_key')
    connection = sql.connect(
        server_hostname='adb-226745366320813.13.azuredatabricks.net',
        http_path='/sql/1.0/endpoints/61b1bce9adcc6384',
        access_token='dapid4721dc6ca8b37c502db929a880cb2a1')

    # create cursor
    cursor = connection.cursor()

    group_key = group_key + '%'
    query = "SELECT Tracking.Parameter, Tracking.Point_Estimate, Tracking.Standard_Error, Tracking.Lower_CI, Tracking.Upper_CI, Tracking.group_key, Tracking.Date_of_computation FROM dev_hvdb_dm_weibull_tool.hvtb_tg_weibull_test_tracking as Tracking WHERE Tracking.group_key LIKE %s"
    cursor.execute(query, (group_key,))

    df = pd.DataFrame(cursor.fetchall(), columns=['Parameter', 'Point_Estimate', 'Standard_Error', 'Lower_CI', 'Upper_CI', 'group_key', 'Date_of_computation'])
    #df.info()
    df['Parameter'] = df['Parameter'].astype('str')
    df['Point_Estimate'] = df['Point_Estimate'].astype('str')
    df['Standard_Error'] = df['Standard_Error'].astype('str')
    df['Lower_CI'] = df['Lower_CI'].astype('str')
    df['Upper_CI'] = df['Upper_CI'].astype('str')
    df['group_key'] = df['group_key'].astype('str')
    df['Date_of_computation'] = df['Date_of_computation'].astype('str')

    cursor.close()
    connection.close()
    return Response(json.dumps(df.to_dict('records'), sort_keys=False, indent=2, default=default), content_type='application/json; charset=utf-8')

#################################################################################

# GET /misPlotByGroupID/{group_key}/{prod_year} : Join and Get tracking and Failure Grouped details
@app.route("/misPlotByGroupID/<string:group_key>/<string:prod_year>", methods=['GET'])
@cross_origin(supports_credentials=True, methods=['GET'], allow_headers=['Content-Type', 'Access-Control-Allow-Origin'])
def misPlotByGroupID(group_key, prod_year):
    connection = sql.connect(
        server_hostname='adb-226745366320813.13.azuredatabricks.net',
        http_path='/sql/1.0/endpoints/61b1bce9adcc6384',
        access_token='dapid4721dc6ca8b37c502db929a880cb2a1')

    # create cursor
    cursor = connection.cursor()

    group_key = group_key + '%'
    #prod_year = prod_year + '%'

    query = "SELECT FailureGrouped.strata_unique_id, FailureGrouped.vehicle_production_date, FailureGrouped.vehi_engine_series, FailureGrouped.days_to_failure, FailureGrouped.month_to_failure, FailureGrouped.mis, FailureGrouped.group_key AS group_key, ProdVolumeGrouped.vin, ProdVolumeGrouped.vehicle_production_date, ProdVolumeGrouped.vehicle_series, ProdVolumeGrouped.prod_dt_year, ProdVolumeGrouped.prod_dt_month, ProdVolumeGrouped.production_month, ProdVolumeGrouped.key_merge FROM dev_hvdb_dm_weibull_tool.hvtb_tg_weibull_test_failure_grouped as FailureGrouped, dev_hvdb_dm_weibull_tool.hvtb_tg_weibull_test_prod_volume_grouped as ProdVolumeGrouped WHERE FailureGrouped.group_key LIKE %s AND ProdVolumeGrouped.prod_dt_year=%s AND ProdVolumeGrouped.key_merge=FailureGrouped.key_merge"
    cursor.execute(query, (group_key, prod_year))

    # create dataframe
    createDF = pd.DataFrame(cursor.fetchall(),
                            columns=['strata_unique_id', 'vehicle_production_date', 'vehi_engine_series',
                                     'days_to_failure', 'month_to_failure', 'mis', 'group_key', 'vin',
                                     'vehicle_production_date', 'vehicle_series', 'prod_dt_year', 'prod_dt_month',
                                     'production_month', 'key_merge'])

    if createDF.empty:
        return "300"

    # select required columns for MIS
    filterDF = createDF.loc[:,['vin','mis','prod_dt_month','prod_dt_year','production_month', 'strata_unique_id']]

    # calculate Production_volume for each mis
    productionvolumeDF = filterDF.copy()
    productionvolumeDF = \
    productionvolumeDF.groupby(['prod_dt_year', 'prod_dt_month', 'production_month', 'mis'], as_index=False)[
        'vin'].nunique()
    productionvolumeDF['Production_volume'] = productionvolumeDF['vin']
    productionvolumeDF = productionvolumeDF.drop('vin', axis=1)
    productionvolumeDF['prod_dt_month'] = pd.to_numeric(productionvolumeDF['prod_dt_month'])
    productionvolumeDF = productionvolumeDF.sort_values(by=['prod_dt_year', 'prod_dt_month', 'mis'])
    productionvolumeDF.reset_index(drop=True, inplace=True)

    # calculate Failure & cumulative_mis_group for each mis
    failureDF = filterDF.copy()
    failureDF = failureDF.groupby(['prod_dt_year', 'prod_dt_month', 'production_month', 'mis'], as_index=False)[
        'strata_unique_id'].nunique()
    failureDF['Failure'] = failureDF['strata_unique_id']
    failureDF = failureDF.drop('strata_unique_id', axis=1)
    failureDF['prod_dt_month'] = pd.to_numeric(failureDF['prod_dt_month'])
    failureDF = failureDF.sort_values(by=['prod_dt_year', 'prod_dt_month', 'mis'])
    failureDF['cumulative_mis_group'] = failureDF.groupby(["prod_dt_year", "prod_dt_month"])['mis'].cumsum()
    failureDF.reset_index(drop=True, inplace=True)

    # merge productionvolumeDF & failureDF to calculate rate_of_failure
    mergeDF = pd.concat([productionvolumeDF, failureDF], axis=1, join='inner')
    mergeDF = mergeDF.loc[:, ~mergeDF.columns.duplicated()]

    # calculate rate_of_failure
    #mergeDF['rate_of_failure'] = mergeDF.iloc[:, 6] / mergeDF.iloc[:, 4]
    mergeDF['rate_of_failure'] = mergeDF['cumulative_mis_group'] / mergeDF['Production_volume']

    # drop un-used columns
    plotDF = mergeDF.drop(['prod_dt_year', 'prod_dt_month', 'Failure', 'cumulative_mis_group'],axis = 1)

    # find year,month & day from production_month
    plotDF['year'] = plotDF['production_month'].dt.year
    plotDF['month'] = plotDF['production_month'].dt.month
    plotDF['day'] = plotDF['production_month'].dt.day

    # filter dataframe based upon year value
    #plotDF = plotDF[plotDF['year'].isin(year_options)]
    #plotDF = plotDF.loc[plotDF['year'] == int(search_year)]

    # pivot dataframe based upon mis values
    pivotsecondtplotDF = pd.pivot(plotDF, index=['production_month', 'Production_volume'], columns='mis',
                                  values='rate_of_failure')
    pivotsecondtplotDF = pivotsecondtplotDF.fillna(0)

    pivotsecondtplotDF.reset_index(level=0, inplace=True)
    pivotsecondtplotDF.reset_index(level=0, inplace=True)

    # convert dataframe into json format
    #x_values = pivotsecondtplotDF['production_month'].to_numpy()
    x_values = pivotsecondtplotDF['production_month'].astype('str')
    y_values = pivotsecondtplotDF['Production_volume'].to_numpy()
    try:
        y1_values = pivotsecondtplotDF[3].to_numpy()
    except:
        a = 0
    try:
        y2_values = pivotsecondtplotDF[6].to_numpy()
    except:
        a = 0
    try:
        y3_values = pivotsecondtplotDF[9].to_numpy()
    except:
        a = 0
    try:
        y4_values = pivotsecondtplotDF[12].to_numpy()
    except:
        a = 0
    try:
        y5_values = pivotsecondtplotDF[18].to_numpy()
    except:
        a = 0
    try:
        y6_values = pivotsecondtplotDF[24].to_numpy()
    except:
        a = 0
    try:
        y7_values = pivotsecondtplotDF[36].to_numpy()
    except:
        a = 0
    try:
        y8_values = pivotsecondtplotDF[48].to_numpy()
    except:
        a = 0
    try:
        y9_values = pivotsecondtplotDF[60].to_numpy()
    except:
        a = 0
    try:
        intermediateDF = [{'X_Values': x_values, 'Y_Values': y_values, 'Y1_Values': y1_values, 'Y2_Values': y2_values,
                           'Y3_Values': y3_values, 'Y4_Values': y4_values, 'Y5_Values': y5_values,
                           'Y6_Values': y6_values, 'Y7_Values': y7_values}]
    except:
        a = 0
    try:
        intermediateDF = [{'X_Values': x_values, 'Y_Values': y_values, 'Y1_Values': y1_values, 'Y2_Values': y2_values,
                           'Y3_Values': y3_values, 'Y4_Values': y4_values, 'Y5_Values': y5_values,
                           'Y6_Values': y6_values, 'Y7_Values': y7_values, 'Y8_Values': y8_values}]
    except:
        a = 0
    try:
        intermediateDF = [{'X_Values': x_values, 'Y_Values': y_values, 'Y1_Values': y1_values, 'Y2_Values': y2_values,
                           'Y3_Values': y3_values, 'Y4_Values': y4_values, 'Y5_Values': y5_values,
                           'Y6_Values': y6_values, 'Y7_Values': y7_values, 'Y8_Values': y8_values,
                           'Y9_Values': y9_values}]
    except:
        a = 0

    # Convert dictionary to Pandas dataframe
    pandasDF = pd.DataFrame(intermediateDF)
    jsonDF = pandasDF.to_json(orient='records')

    #df_final = pd.DataFrame(intDictionary)
    #return (jsonify(df_final.to_json(orient='records')))
    return (jsonify(jsonDF))

#################################################################################

# GET /timeSeriesByGroupID/{group_key} : Join and Get tracking and Failure Grouped details
@app.route("/timeSeriesByGroupID/<string:group_key>", methods=['GET'])
@cross_origin(supports_credentials=True, methods=['GET'], allow_headers=['Content-Type', 'Access-Control-Allow-Origin'])
def timeSeriesByGroupID(group_key):
    connection = sql.connect(
    server_hostname='adb-226745366320813.13.azuredatabricks.net',
    http_path='/sql/1.0/endpoints/61b1bce9adcc6384',
    access_token='dapid4721dc6ca8b37c502db929a880cb2a1')

    # create cursor
    cursor = connection.cursor()

    #Extract groupkey format from "07206_47_2018-01-01" to "07206_47"
    #group_key = group_key[0:-14]
    group_key = group_key + '%'

    query = "SELECT Tracking.Parameter, Tracking.Point_Estimate, Tracking.group_key FROM dev_hvdb_dm_weibull_tool.hvtb_tg_weibull_test_tracking as Tracking WHERE Tracking.group_key LIKE %s AND Tracking.Parameter IN ('Alpha', 'Beta', 'Gamma')"
    cursor.execute(query,(group_key,))

    df = pd.DataFrame(cursor.fetchall(), columns=['Parameter', 'Point_Estimate', 'group_key'])

    if df.empty:
        return "300"

    df['group_key'] = df['group_key'].astype(str)
    df['production_month'] = df['group_key'].str.split('_', 3).str[2]
    df["production_month"] = df["production_month"].str.strip("-")

    df = pd.pivot_table(df, index='production_month', columns='Parameter', values='Point_Estimate',
                         aggfunc=np.sum)
    df.reset_index(level=0, inplace=True)

    df['Gamma'] = df['Gamma'].fillna(0)
    df['production_month'] = pd.to_datetime(df['production_month'])
    df['production_month'] = df['production_month'].apply(lambda x: x.strftime('%Y-%m-%d'))

    try:
        g_estimate = df['Gamma'].tolist()
    except:
        a = 0
    try:
        b_estimate = df['Beta'].tolist()
    except:
        a = 0
    try:
        a_estimate = df['Alpha'].tolist()
    except:
        a = 0

    prod_month = df['production_month'].tolist()

    try:
        g_esti = {"Point_Estimate": g_estimate}
    except:
        a = 0
    try:
        b_esti = {"Point_Estimate": b_estimate}
    except:
        a = 0
    try:
        a_esti = {"Point_Estimate": a_estimate}
    except:
        a = 0
    try:
        final_json = {"Gamma": g_esti, "x": prod_month}
    except:
        a = 0
    try:
        final_json = {"Beta": b_esti, "Gamma": g_esti, "x": prod_month}
    except:
        a = 0
    try:
        final_json = {"Alpha": a_esti, "Beta": b_esti, "Gamma": g_esti, "x": prod_month}
    except:
        a = 0

    return Response(json.dumps(final_json, sort_keys=False, indent=2, default=default),
                    content_type='application/json; charset=utf-8')

##############################################################

# GET /weibullPlotByGroupID/{group_key} : Join and Get tracking and Failure Grouped details
@app.route("/weibullPlotByGroupID/<string:group_key>", methods=['GET'])
@cross_origin(supports_credentials=True, methods=['GET'], allow_headers=['Content-Type', 'Access-Control-Allow-Origin'])
def weibullPlotByGroupID(group_key):
    connection = sql.connect(
    server_hostname='adb-226745366320813.13.azuredatabricks.net',
    http_path='/sql/1.0/endpoints/61b1bce9adcc6384',
    access_token='dapid4721dc6ca8b37c502db929a880cb2a1')

    # create cursor
    cursor = connection.cursor()

    #Extract groupkey format from "07206_47_2018-01-01" to "07206_47"
    #group_key = group_key[0:-14]
    group_key = group_key + '%'

    #query = "SELECT Tracking.Parameter, Tracking.Point_Estimate, Tracking.group_key, Tracking.Date_of_computation,  FailureGrouped.vehicle_production_date, FailureGrouped.days_to_failure, FailureGrouped.month_to_failure,  FailureGrouped.group_key AS f_group_key, ProdVolumeGrouped.vin, ProdVolumeGrouped.vehicle_production_date,  ProdVolumeGrouped.prod_dt_year, ProdVolumeGrouped.prod_dt_month, ProdVolumeGrouped.production_month, ProdVolumeGrouped.key_merge FROM dev_hvdb_dm_weibull_tool.hvtb_tg_weibull_test_tracking as Tracking, dev_hvdb_dm_weibull_tool.hvtb_tg_weibull_test_failure_grouped as FailureGrouped, dev_hvdb_dm_weibull_tool.hvtb_tg_weibull_test_prod_volume_grouped as ProdVolumeGrouped WHERE Tracking.group_key LIKE %s AND ProdVolumeGrouped.prod_dt_year LIKE %s AND FailureGrouped.group_key=Tracking.group_key AND ProdVolumeGrouped.key_merge=FailureGrouped.key_merge AND Tracking.Parameter IN ('Alpha', 'Beta', 'Gamma') LIMIT %d"
    #query = "SELECT Tracking.Parameter, Tracking.Point_Estimate, Tracking.group_key, Tracking.Date_of_computation,  FailureGrouped.vehicle_production_date, FailureGrouped.days_to_failure, FailureGrouped.month_to_failure,  FailureGrouped.group_key AS f_group_key, ProdVolumeGrouped.vin, ProdVolumeGrouped.vehicle_production_date,  ProdVolumeGrouped.prod_dt_year, ProdVolumeGrouped.prod_dt_month, ProdVolumeGrouped.production_month, ProdVolumeGrouped.key_merge FROM dev_hvdb_dm_weibull_tool.hvtb_tg_weibull_test_tracking as Tracking, dev_hvdb_dm_weibull_tool.hvtb_tg_weibull_test_failure_grouped as FailureGrouped, dev_hvdb_dm_weibull_tool.hvtb_tg_weibull_test_prod_volume_grouped as ProdVolumeGrouped WHERE Tracking.group_key LIKE %s AND ProdVolumeGrouped.prod_dt_year LIKE %s AND FailureGrouped.group_key=Tracking.group_key AND ProdVolumeGrouped.key_merge=FailureGrouped.key_merge AND Tracking.Parameter IN ('Alpha', 'Beta', 'Gamma') LIMIT %d"
    query = "SELECT Tracking.Parameter, Tracking.Point_Estimate, Tracking.group_key FROM dev_hvdb_dm_weibull_tool.hvtb_tg_weibull_test_tracking as Tracking WHERE Tracking.group_key LIKE %s AND Tracking.Parameter IN ('Alpha', 'Beta', 'Gamma')"
    cursor.execute(query,(group_key,))

    df = pd.DataFrame(cursor.fetchall(), columns=['Parameter', 'Point_Estimate', 'group_key'])

    if df.empty:
        return "300"

    df['group_key'] = df['group_key'].astype(str)
    df['prod_month'] = df['group_key'].str.split('_', 3).str[2]
    df["prod_month"] = df["prod_month"].str.strip("-")

    #df = df[df['prod_month'].str.contains('2017')]
    groupDict = df.groupby('Parameter').apply(lambda g: g.drop('Parameter', axis=1).to_dict(orient='list')).to_dict()

    dict_1 = collections.defaultdict(dict)
    x = []
    for key, value in list(groupDict.items()):
        for key_1, value_1 in list(value.items()):
            #if 'prod_month' in key_1:
            #    x = value_1
            #    del value[key_1]
            if key_1 == 'prod_month':
                x = value_1

            if key_1 != 'prod_month':
                dict_1[key][key_1] = value_1

    dict_1['x'] = x
    #print(dict_1)

    #return (jsonify(final_json))
    cursor.close()
    connection.close()
    #return Response(json.dumps(df.to_dict('records'), sort_keys=False, indent=2, default=default), content_type='application/json; charset=utf-8')
    return Response(json.dumps(dict_1, sort_keys=False, indent=2, default=default),
                    content_type='application/json; charset=utf-8')

##############################################################
def default(o):
    if isinstance(o, (datetime.date, datetime.datetime, datetime.time)):
        return o.__str__()

##############################################################

# main
if __name__ == '__main__':
	app.debug = True
	app.run()
    #app.run(host='0.0.0.0', port=4000)
	

##############################################################
