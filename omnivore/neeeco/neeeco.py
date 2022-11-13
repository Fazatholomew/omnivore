import pandas as pd
import re

def neeeco(neeeco_input: pd.DataFrame ,neeeco_wx_input: pd.DataFrame):
    neeeco_output = pd.merge(left=neeeco_input, right=neeeco_wx_input, 
                            how='left', left_on='Related to', right_on='HEA - Last, First, Address')

    # // Neeeco words into Salesforce Stage
    stageMapper = {
    'Customer Declined - No F/U': 'Recommended - Unsigned',
    'Deferred Until Later': 'No Opportunity',
    'High Probability': 'Recommended - Unsigned',
    'Low Probability': 'Recommended - Unsigned',
    'Medium Probability': 'Recommended - Unsigned',
    'No Opportunity': 'No Opportunity',
    'Signed & Sold': 'Signed Contracts',
    'Sold then Canceled': 'Recommended - Unsigned',
    'OPS H&S Follow Up': 'Health & Safety Barrier'
    }

    # // Cancelation Reasons
    neeecoCancelMapper = {
    'Condo - Complex': '5+ units',
    'Unable to Contact/8 attempts': 'Unresponsive',
    'Does Not Want Audit': 'Not Interested',
    'Low Income': 'Low Income',
    'Municipal - Not NG or EV': 'Unresponsive',
    'Already Had Audit (2year)': 'Had HEA within 2 years',
    'Bad Data': 'Bad Data',
    'No Account Info': 'Bad Data',
    'Scheduling Conflict': 'Reschedule Request',
    '': 'No Reason'
    }

    # // Health And Safety Issues
    hsMapper = {
    'K&T': 'Knob & Tube (Major)',
    'CST': 'Combustion Safety Failure',
    'Moisture': 'Mold/Moisture',
    'Asbestos': 'Asbestos'
    }

    # // Health And Safety Statuses
    hsStageMapper = {
    'H&S Remediated': 'Barrier Removed',
    'H&S No Follow Up Needed': 'Barrier Removed',
    'H&S Needs Follow Up': 'Not Attempted'
    }

    neeeco_output=neeeco_output.rename(columns={"ID": "ID_from_HPC__c"})

    #     // Combine both data
    neeeco_output['Final_Contract_Amount__c']=neeeco_output[
        'Final Full Job Amount Invoiced'].combine_first(neeeco_output['Customer Final Payment Collected'])
    neeeco_output['Insulation Project Status']=neeeco_output[
        'Insulation Project Status_y'].combine_first(neeeco_output['Insulation Project Status_x'])
    neeeco_output['Contract_Amount__c']=neeeco_output[
        '$ Total Weatherization Sold_y'].combine_first(neeeco_output['$ Total Weatherization Sold_x'])
    neeeco_output['Insulation Project Installation Date']=neeeco_output[
        'Insulation Project Installation Date_y'].combine_first(neeeco_output[
            'Insulation Project Installation Date_x'])
    neeeco_output['Health & Safety Status']=neeeco_output[
        'Health & Safety Status_y'].combine_first(neeeco_output['Health & Safety Status_x'])

    neeeco_output['Final_Contract_Amount__c']=neeeco_output['Final_Contract_Amount__c'].fillna('')
    neeeco_output['Contract_Amount__c']=neeeco_output['Contract_Amount__c'].fillna('')

    #       // Skip Empty Row
    neeeco_output = neeeco_output[neeeco_output['ID_from_HPC__c'].notnull()]

    neeeco_output["Street__c"] = neeeco_output[
        "Full Address including zip"].str.extract(r'([a-zA-Z]\w{2,} \w{1,})')
    neeeco_output["Unit__c"] = neeeco_output[
        "Full Address including zip"].str.extract(r'^(\d{1,})')
    neeeco_output["City__c"] = neeeco_output[
        "Full Address including zip"].str.extract(r'((?<=:\s|,\s)\b\w+\s*\w{2}\b)')
    neeeco_output["State__c"] = neeeco_output["Full Address including zip"].str.extract(r"([A-Z]{2})")
    neeeco_output["Zipcode__c"] = neeeco_output["Full Address including zip"].str.extract(r"([0-9]{5,6}$)")
    neeeco_output['Name'] = neeeco_output['Name'].str.replace('(\d+\w{1}$)', '')

    neeeco_output['Date of Audit'] = pd.to_datetime(neeeco_output['Date of Audit'])
    neeeco_output['Date of Audit'] = neeeco_output['Date of Audit'].dt.strftime('%Y-%m-%d')
    neeeco_output['Date of Audit'] = neeeco_output['Date of Audit'].astype(str)
    neeeco_output['HEA_Date_And_Time__c'] = neeeco_output['Date of Audit']+'T00:00:00.000-07:00'
    neeeco_output.loc[neeeco_output['HEA_Date_And_Time__c'] == 'nanT00:00:00.000-07:00', 
                                                            'HEA_Date_And_Time__c'] = ''

    neeeco_output['Date Scheduled in Vcita'] = pd.to_datetime(neeeco_output['Date Scheduled in Vcita'])
                                                            # , 
                                                            # format='%m/%d/%y')
    neeeco_output['Date Scheduled in Vcita'] = neeeco_output['Date Scheduled in Vcita'].dt.strftime('%Y-%m-%d')
    neeeco_output['Date Scheduled in Vcita'] = neeeco_output['Date Scheduled in Vcita'].astype(str)
    neeeco_output['Date Scheduled in Vcita']=neeeco_output['Date Scheduled in Vcita']+'T00:00:00.000-07:00'
    neeeco_output['Created'] = pd.to_datetime(neeeco_output['Created'])
    neeeco_output['Created'] = neeeco_output['Created'].dt.strftime('%Y-%m-%d')
    neeeco_output['Created'] = neeeco_output['Created'].astype(str)
    neeeco_output['Created']=neeeco_output['Created']+'T00:00:00.000-07:00'
    neeeco_output['CloseDate']=neeeco_output[
        'Date Scheduled in Vcita'].combine_first(neeeco_output['Created'])
    neeeco_output.loc[neeeco_output['CloseDate'] == 'nanT00:00:00.000-07:00', 
                    'CloseDate'] = neeeco_output['Created']

    neeeco_output['StageName'] = 'Scheduled'
    neeeco_output['Cancelation_Reason_s__c'] = ''

    #         // if canceled or disqualified
    neeeco_output.loc[neeeco_output['Lead Status'] != 'Scheduled (Lead Converted)', 
                                                    'StageName'] = 'Canceled'
    neeeco_output.loc[neeeco_output['Lead Status'] != 'Scheduled (Lead Converted)', 
                    'Cancelation_Reason_s__c'] = neeeco_output['Lead Disqualified'].map(neeecoCancelMapper)
                    
    #         // Defaulted to No Reason
    neeeco_output.loc[(neeeco_output['Lead Status'] != 'Scheduled (Lead Converted)') & 
                    (neeeco_output['Lead Disqualified'] == ''), 
                    'Cancelation_Reason_s__c'] = 'No Reason'

    neeeco_output['Health_Safety_Barrier_Status__c'] = neeeco_output[
        'Health & Safety Status'].map(hsStageMapper)
    neeeco_output['Health_Safety_Barrier_Status__c']=neeeco_output['Health_Safety_Barrier_Status__c'].fillna('')

        
    neeeco_output['Health & Safety Issue']=neeeco_output['Health & Safety Issue'].fillna('')
    neeeco_output['Health_Safety_Barrier__c'] = (neeeco_output['Health & Safety Issue']
                                            .str.replace('K&T', 'Knob & Tube (Major)')
                                            .str.replace('CST', 'Combustion Safety Failure')
                                            .str.replace('Moisture', 'Mold/Moisture')
                                            .str.replace('Asbestos', 'Asbestos')
                                            .str.replace('', '')
                                            .str.split(",")
                                            .str.join(';')
                                            .astype(str))

    # // Audit completed
    neeeco_output.loc[(neeeco_output['HEA Status'] == 'Completed') |
                    (neeeco_output['Insulation Project Status'].notnull()), 
                    'StageName'] = neeeco_output['Insulation Project Status'].map(stageMapper)
    neeeco_output.loc[(neeeco_output['HEA Status'] == 'Completed') &
                    (neeeco_output['Insulation Project Status'].isnull()),
                    'StageName'] = 'No Opportunity'

    # # // Although contract signed, still barier
    neeeco_output.loc[neeeco_output['Health & Safety Status'] == 'H&S Needs Follow Up', 
                    'StageName'] = 'Health & Safety Barrier'

    #       // Wx Jobs Statuses
    neeeco_output['Weatherization_Status__c'] = ''
    neeeco_output.loc[neeeco_output['StageName'] == 'Signed Contracts', 
                    'Weatherization_Status__c'] = 'Not Yet Scheduled'

    neeeco_output['Insulation Project Installation Date'] = pd.to_datetime(
        neeeco_output['Insulation Project Installation Date'])
    neeeco_output['Insulation Project Installation Date'] = neeeco_output[
        'Insulation Project Installation Date'].dt.strftime('%Y-%m-%d')
    neeeco_output['Insulation Project Installation Date'] = neeeco_output[
        'Insulation Project Installation Date'].astype(str)
    neeeco_output['Weatherization_Date_Time__c'] = neeeco_output[
        'Insulation Project Installation Date']+'T00:00:00.000-07:00'
    neeeco_output.loc[neeeco_output['Weatherization_Date_Time__c'] == 'nanT00:00:00.000-07:00', 
                    'Weatherization_Date_Time__c'] = ''

    neeeco_output.loc[(neeeco_output['StageName'] == 'Signed Contracts') & 
                    (neeeco_output['Weatherization_Date_Time__c'].notnull()), 
                    'Weatherization_Status__c'] = 'Scheduled'
    neeeco_output.loc[(neeeco_output['StageName'] == 'Signed Contracts') & 
                    (neeeco_output['Final_Contract_Amount__c'].notnull()), 
                    'Weatherization_Status__c'] = 'Completed'

    # // No cancel reason, default it to No Reason
    neeeco_output.loc[(neeeco_output['StageName'] == 'Canceled') & 
                    (neeeco_output['Cancelation_Reason_s__c'] == ''), 
                    'Cancelation_Reason_s__c'] = 'No Reason'

    #         // If date is still in the future, stage is scheduled
    neeeco_output['Date of Audit'] = pd.to_datetime(neeeco_output['Date of Audit'])

    neeeco_output.loc[neeeco_output['Date of Audit'] == '', 'Date of Audit'] = pd.to_datetime("today")
    neeeco_output.loc[neeeco_output['Date of Audit'] > pd.to_datetime("today"), 'StageName'] = 'Scheduled'

    #       // VHEA detection
    neeeco_output['isVHEA__c'] = False
    neeeco_output.loc[neeeco_output['Related to'].str.contains('VHEA').fillna(False), 'isVHEA__c'] = True

    neeeco_output['HPC__c'] = '0013i00000AtGAvAAN'

    for i in neeeco_output['Phone'].index:
        neeeco_output['Phone'][i] = re.sub(r'[^0-9]', '', str(neeeco_output['Phone'][i]))
        if len(neeeco_output['Phone'][i])<10:
            neeeco_output['Phone'][i]=''

    neeeco_output=neeeco_output.loc[:,['Street__c','Unit__c','City__c','State__c','Zipcode__c','Name',
                                    'HEA_Date_And_Time__c','CloseDate','StageName',
                                    'Health_Safety_Barrier_Status__c','Health_Safety_Barrier__c',
                                    'isVHEA__c','Weatherization_Status__c','Weatherization_Date_Time__c',
                                    'Contract_Amount__c','Final_Contract_Amount__c','ID_from_HPC__c',
                                    'Cancelation_Reason_s__c','HPC__c', 'Phone']]

    return neeeco_output