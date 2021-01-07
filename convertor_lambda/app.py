import json
import os
import re
from io import StringIO
from urllib.parse import unquote_plus
import boto3
import pandas as pd

s3 = boto3.resource('s3')
csvBuffer = StringIO()

outputFolder = os.environ.get('OUTPUT_FOLDER')
failedFolder = os.environ.get('FAILED_FOLDER')
todoFolder = os.environ.get('TODO_FOLDER')
customer01 = os.environ.get('CUSTOMER_01')


def Validate_elec(Datafile, bucket, customer):
    df1 = Datafile
    df2 = pd.DataFrame()

    df2['Code'] = df1['Nmi']
    df2['Invoice_Number'] = df1['InvoiceNumber']
    df2['Cost'] = df1['TotalChargeIncExtra']
    df2['Date'] = df1['EndDate'].apply(pd.to_datetime, format='%Y-%m-%d')
    df2['Date_TaxPoint'] = df1['IssueDate'].apply(pd.to_datetime, format='%Y-%m-%d')
    df2['Date_Previous'] = df1['StartDate'].apply(pd.to_datetime, format='%Y-%m-%d')

    # Capturing VAT
    All_VAT_list = []
    VAT_list = []
    for index, row in df1.iterrows():
        for vatitem in range(len(row['ExtraCharges'])):
            VAT1 = row['ExtraCharges'][vatitem]
            if VAT1['Name'] == 'VAT':
                VAT_list.append(VAT1['Cost'])
        BillVAT = sum(VAT_list)
        All_VAT_list.append(BillVAT)
    df2['VAT'] = pd.DataFrame(All_VAT_list)

    # Capturing Standing Charges
    Standingcharge_list = []
    Standingchargerate_list = []
    substring = "Standing Charge"
    for index, row in df1.iterrows():
        stchrge1 = row['Periods']

        if len(stchrge1) > 0:
            for schargenum in range(len(stchrge1)):
                try:
                    stchrgeX = stchrge1[schargenum]
                    stcharge2 = stchrgeX['EnergyService']

                    for stnum in range(len(stcharge2)):
                        stchrge3 = stcharge2[stnum]
                        fullstring = stchrge3['Comment']

                        if (stchrge3['Name'] == 'Service') & (substring in fullstring):
                            Standingcharge_list.append(stchrge3['Cost'])
                            Standingchargerate_list.append(stchrge3['Price'])
                except:
                    print('No Standing charges')
    try:
        df2['Standing_Charge'] = pd.DataFrame(Standingcharge_list)
        df2['Standing_Charge_Rate'] = pd.DataFrame(Standingchargerate_list)
    except:
        df2['Standing_Charge'] = ''
        df2['Standing_Charge_Rate'] = ''

    ###########################################################################
    # # capturing KVA charges
    ###########################################################################
    KVA_Cost_list = []
    KVA_costrate_list = []
    KVA_units = []

    for index, row in df1.iterrows():
        KVA1 = row['Periods']

        if len(KVA1) > 0:
            for kvaitem in range(len(KVA1)):
                try:
                    KVAX = KVA1[kvaitem]
                    KVA2 = KVAX['Capacities']

                    for kvanum in range(len(KVA2)):
                        KVA3 = KVA2[kvanum]
                        if KVA3['Name'] == 'DUoS':
                            KVA_Cost_list.append(KVA3['Cost'])
                            KVA_units.append(KVA3['Quantity'])
                            KVA_costrate_list.append(KVA3['Price'])
                except:
                    print('No KVA charges')
    try:
        df2['kVA'] = pd.DataFrame(KVA_units)
        df2['kVA_Cost_Rate'] = pd.DataFrame(KVA_costrate_list)
        df2['kVA_Cost'] = pd.DataFrame(KVA_Cost_list)
    except:
        df2['kVA'] = ''
        df2['kVA_Cost_Rate'] = ''
        df2['kVA_Cost'] = ''

    ###########################################################################
    # Capturing Peak Previous and Current Meter Reads + Estimate
    ###########################################################################
    M1_Previousread_list = []
    M1_CurrentRead_list = []
    M1_CurrentDate_list = []
    M1_PreviousDate_list = []
    M1_CurrentReadType_list = []
    M2_Previousread_list = []
    M2_PreviousDate_list = []
    M2_CurrentRead_list = []
    M2_CurrentDate_list = []

    for index, row in df1.iterrows():
        Meters1 = row['Meters']
        meterno = len(Meters1)

        if meterno == 0:
            M1_Previousread_list.append('')
            M1_PreviousDate_list.append('')
            M1_CurrentRead_list.append('')
            M1_CurrentDate_list.append('')
            M1_CurrentReadType_list.append('A')
            M2_Previousread_list.append('')
            M2_PreviousDate_list.append('')
            M2_CurrentRead_list.append('')
            M2_CurrentDate_list.append('')
        elif meterno == 1:
            M1_Previousread_list.append(Meters1[0]['PreviousRead'])
            M1_CurrentRead_list.append(Meters1[0]['CurrentRead'])
            M1_CurrentDate_list.append(Meters1[0]['CurrentReadDate'])
            if Meters1[0]['CurrentReadType'] == 'Actual':
                M1_CurrentReadType_list.append('A')
            elif Meters1[0]['CurrentReadType'] == 'Estimated':
                M1_CurrentReadType_list.append('E')
            else:
                M1_CurrentReadType_list.append('A')

            M2_Previousread_list.append('')
            M2_PreviousDate_list.append('')
            M2_CurrentRead_list.append('')
            M2_CurrentDate_list.append('')

        elif meterno == 2:
            M1_Previousread_list.append(Meters1[0]['PreviousRead'])
            M1_CurrentRead_list.append(Meters1[0]['CurrentRead'])
            M1_CurrentDate_list.append(Meters1[0]['CurrentReadDate'])
            if Meters1[0]['CurrentReadType'] == 'Actual':
                M1_CurrentReadType_list.append('A')
            elif Meters1[0]['CurrentReadType'] == 'Estimated':
                M1_CurrentReadType_list.append('E')
            else:
                M1_CurrentReadType_list.append('A')

            M2_Previousread_list.append(Meters1[1]['PreviousRead'])
            M2_PreviousDate_list.append(Meters1[1]['PreviousReadDate'])
            M2_CurrentRead_list.append(Meters1[1]['CurrentRead'])
            M2_CurrentDate_list.append(Meters1[1]['CurrentReadDate'])

    df2['M1_Present'] = pd.DataFrame(M1_CurrentRead_list)
    df2['M1_Previous'] = pd.DataFrame(M1_Previousread_list)
    df2['M1_Read_Date'] = pd.DataFrame(M1_CurrentDate_list)
    df2['Estimate'] = pd.DataFrame(M1_CurrentReadType_list)
    df2['M2_Present'] = pd.DataFrame(M2_CurrentRead_list)
    df2['M2_Previous'] = pd.DataFrame(M2_Previousread_list)
    df2['M2_Read_Date'] = pd.DataFrame(M2_CurrentDate_list)

    ###########################################################################
    # Capturing Peak charges
    ###########################################################################
    PeakChg_list = []
    PeakQty_list = []
    PeakRate_list = []
    OffPeakChg_list = []
    OffPeakQty_list = []
    OffPeakRate_list = []

    for index, row in df1.iterrows():
        peak1 = row['Periods']

        if len(peak1) > 0:
            for energyitem in range(len(peak1)):
                try:
                    peakx = peak1[energyitem]
                    peak2 = peakx['EnergyLineItems']

                    for finalpeak in range(len(peak2)):
                        peak3 = peak2[finalpeak]
                        if (peak3['Name'] == 'Peak') | (peak3['Name'] == 'Energy'):
                            PeakChg_list.append(peak3['Cost'])
                            PeakQty_list.append(peak3['Quantity'])
                            PeakRate_list.append(peak3['Price'])
                        elif peak3['Name'] == 'OffPeak':
                            OffPeakChg_list.append(peak3['Cost'])
                            OffPeakQty_list.append(peak3['Quantity'])
                            OffPeakRate_list.append(peak3['Price'])
                except:
                    print("No energy items")
    try:
        df2['M1_Units'] = pd.DataFrame(PeakQty_list)
        df2['M1_Cost_Rate'] = pd.DataFrame(PeakRate_list)
        df2['M1_Cost'] = pd.DataFrame(PeakChg_list)
    except:
        df2['M1_Units'] = ''
        df2['M1_Cost_Rate'] = ''
        df2['M1_Cost'] = ''
    try:
        df2['M2_Units'] = pd.DataFrame(OffPeakQty_list)
        df2['M2_Cost_Rate'] = pd.DataFrame(PeakRate_list)
        df2['M2_Cost'] = pd.DataFrame(OffPeakChg_list)
    except:
        df2['M2_Units'] = ''
        df2['M2_Cost_Rate'] = ''
        df2['M2_Cost'] = ''

    ############################################################################

    finaldf = df2[
        ['Code', 'Date', 'Invoice_Number', 'Date_TaxPoint', 'Date_Previous', 'M1_Units', 'M1_Cost_Rate', 'M1_Cost',
         'M2_Units', 'M2_Cost_Rate', 'M2_Cost', 'Standing_Charge', 'Standing_Charge_Rate', 'kVA', 'kVA_Cost',
         'kVA_Cost_Rate', 'VAT', 'Cost', 'Estimate', 'M1_Present', 'M1_Previous', 'M2_Present',
         'M2_Previous', 'M1_Read_Date', 'M2_Read_Date']]

    # Writing into a file
    address = re.sub('\W+', '', df1['Address'].iloc[0])
    File_invoicenumber = re.sub('\W+', '', df1['InvoiceNumber'].iloc[0])
    filename = address[0:21] + "-" + str(df1['Nmi'].iloc[0]) + "-" + str(File_invoicenumber)
    finaldf.to_csv(csvBuffer)
    s3.Object(bucket, customer + '/' + outputFolder + '/' + filename + ".csv").put(Body=csvBuffer.getvalue())


def Validate_gas(Datafile, bucket, customer):
    dfgas1 = Datafile
    dfgas2 = pd.DataFrame()

    dfgas2['Code'] = dfgas1['Nmi']
    dfgas2['Invoice_Number'] = dfgas1['InvoiceNumber']
    dfgas2['Cost'] = dfgas1['TotalChargeIncExtra']
    dfgas2['Date'] = dfgas1['EndDate'].apply(pd.to_datetime, format='%Y-%m-%d')
    dfgas2['Date_TaxPoint'] = dfgas1['IssueDate'].apply(pd.to_datetime, format='%Y-%m-%d')
    dfgas2['Date_Previous'] = dfgas1['StartDate'].apply(pd.to_datetime, format='%Y-%m-%d')

    #################################################################################
    # Capturing VAT - GAS
    #################################################################################
    All_VAT_list = []
    VAT_list = []
    for index, row in dfgas1.iterrows():
        for vatitem in range(len(row['ExtraCharges'])):
            VAT1 = row['ExtraCharges'][vatitem]
            if (VAT1['Name'] == 'VAT'):
                VAT_list.append(VAT1['Cost'])
        BillVAT = sum(VAT_list)
        All_VAT_list.append(BillVAT)
    dfgas2['VAT'] = pd.DataFrame(All_VAT_list)

    #################################################################################
    # Capturing standing charge
    #################################################################################

    Standingcharge_list = []
    Standingchargerate_list = []
    substring = "Standing Charge"
    for index, row in dfgas1.iterrows():
        stchrge1 = row['Periods']

        if len(stchrge1) > 0:
            for schargenum in range(len(stchrge1)):
                try:
                    stchrgeX = stchrge1[schargenum]
                    stcharge2 = stchrgeX['EnergyService']

                    for stnum in range(len(stcharge2)):
                        stchrge3 = stcharge2[stnum]
                        fullstring = stchrge3['Comment']

                        if ((stchrge3['Name'] == 'Service') & (substring in fullstring)):
                            Standingcharge_list.append(stchrge3['Cost'])
                            Standingchargerate_list.append(stchrge3['Price'])
                except:
                    print('No Standing charges')
    try:
        dfgas2['Standing_Charge'] = pd.DataFrame(Standingcharge_list)
        dfgas2['Standing_Charge_Rate'] = pd.DataFrame(Standingchargerate_list)
    except:
        dfgas2['Standing_Charge'] = ''
        dfgas2['Standing_Charge_Rate'] = ''

    #################################################################################
    # Capturing Peak Previous and Current Meter Reads + Estimate
    #################################################################################
    M1_Previousread_list = []
    M1_CurrentRead_list = []
    M1_CurrentDate_list = []
    M1_PreviousDate_list = []
    M1_CurrentReadType_list = []
    M1_CorrFact_list = []
    M1_CalVal_list = []

    for index, row in dfgas1.iterrows():
        Meters1 = row['Meters']
        meterno = len(Meters1)

        if meterno == 0:
            M1_Previousread_list.append('')
            M1_PreviousDate_list.append('')
            M1_CurrentRead_list.append('')
            M1_CurrentDate_list.append('')
            M1_CurrentReadType_list.append('A')

        elif meterno == 1:
            M1_Previousread_list.append(Meters1[0]['PreviousRead'])
            M1_CurrentRead_list.append(Meters1[0]['CurrentRead'])
            M1_CurrentDate_list.append(Meters1[0]['CurrentReadDate'])
            M1_CorrFact_list.append(Meters1[0]['CorrectionFactor'])
            M1_CalVal_list.append(Meters1[0]['CalorificValue'])

            if Meters1[0]['CurrentReadType'] == 'Actual':
                M1_CurrentReadType_list.append('A')
            elif Meters1[0]['CurrentReadType'] == 'Estimated':
                M1_CurrentReadType_list.append('E')
            else:
                M1_CurrentReadType_list.append('A')

    dfgas2['M1_Present'] = pd.DataFrame(M1_CurrentRead_list)
    dfgas2['M1_Previous'] = pd.DataFrame(M1_Previousread_list)
    dfgas2['M1_Read_Date'] = pd.DataFrame(M1_CurrentDate_list)
    dfgas2['Estimate'] = pd.DataFrame(M1_CurrentReadType_list)
    dfgas2['M1_Factor_1'] = pd.DataFrame(M1_CorrFact_list)
    dfgas2['M1_Factor_2'] = pd.DataFrame(M1_CalVal_list)

    #################################################################################
    # Capturing Gas units and charges
    #################################################################################

    All_units_list = []
    All_charges_list = []
    Gas_costrate = []
    Gas_units_list = []
    Gas_charges_list = []

    for index, row in dfgas1.iterrows():
        energycharges1 = row['Periods']

        if len(energycharges1) > 0:
            for echargenum in range(len(energycharges1)):
                echargeX = energycharges1[echargenum]
                echarge2 = echargeX['EnergyLineItems']

                for enum1 in range(len(echarge2)):
                    echarge3 = echarge2[enum1]

                    if echarge3['Name'] == 'Energy':
                        Gas_units_list.append(echarge3['Quantity'])
                        Gas_charges_list.append(echarge3['Cost'])
                        gascostrate = echarge3['Price']

                All_units_list.append(sum(Gas_units_list))
                All_charges_list.append(sum(Gas_charges_list))
                Gas_costrate.append(gascostrate)

    dfgas2['M1_Units'] = pd.DataFrame(All_units_list)
    dfgas2['M1_Cost'] = pd.DataFrame(All_charges_list)
    dfgas2['M1_Cost_Rate'] = pd.DataFrame(Gas_costrate)

    ###################################################################################

    dfgas2['kWh_Factor'] = 3.6

    finaldf = dfgas2[['Code', 'Date', 'Invoice_Number', 'Date_TaxPoint', 'Date_Previous', 'M1_Units',
                      'M1_Cost_Rate', 'M1_Cost', 'Standing_Charge', 'Standing_Charge_Rate', 'VAT', 'Cost',
                      'Estimate', 'M1_Present', 'M1_Previous', 'M1_Factor_1', 'M1_Factor_2', 'kWh_Factor',
                      'M1_Read_Date']]

    # Writing into a file
    address = re.sub('\W+', '', dfgas1['Address'].iloc[0])
    File_invoicenumber = re.sub('\W+', '', dfgas1['InvoiceNumber'].iloc[0])
    filename = address[0:21] + "-" + str(dfgas1['Nmi'].iloc[0]) + "-" + str(File_invoicenumber)
    finaldf.to_csv(csvBuffer)
    s3.Object(bucket, customer + '/' + outputFolder + '/' + filename + ".csv").put(Body=csvBuffer.getvalue())


def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        customer = key.split("/")[0]
        folder = key.split("/")[1]
        content_object = s3.Object(bucket, key)

        if folder == todoFolder and customer == customer01:
            try:
                file_content = content_object.get()['Body'].read().decode('utf-8')
                Jsondata = json.loads(file_content)
                Datafile = pd.DataFrame(Jsondata)
                Commodity_value = Datafile['Commodity'].iloc[0]
                print(Commodity_value)
                if Commodity_value == 0:
                    Validate_elec(Datafile, bucket, customer)
                if Commodity_value == 1:
                    Validate_gas(Datafile, bucket, customer)
                s3.Object(bucket, customer + '/' + outputFolder + '/' + key.split("/")[2]).put(
                    Body=content_object.get()['Body'].read())
                s3.Object(bucket, key).delete()
            except:
                s3.Object(bucket, customer + '/' + failedFolder + '/' + key.split("/")[2]).put(
                    Body=content_object.get()['Body'].read())
                s3.Object(bucket, key).delete()

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
