import json
import boto3
def lambda_handler(event, context):
    # TODO implement
    region='us-east-1'
    try:            
        s3=boto3.client('s3')            
        dyndb = boto3.client('dynamodb', region_name=region)
        confile= s3.get_object(Bucket='lti871', Key='prashant/output_historicalData.csv')
        recList = confile['Body'].read().split('\n')
        firstrecord=True
        csv_reader = csv.reader(recList, delimiter=',', quotechar='"')
        for row in csv_reader:
            if (firstrecord):
                firstrecord=False
                continue
            loan_number = row[0]
            country_code = row[1]
            country = row[2]
            loan_type = row[3]
            loan_status = row[4]
            interest_rate = row[5]
            project_id = row[6]
            project_name = row[7]
            orig_prin_amount = row[8]
            cancelled_amount = row[9]
            undisbursed_amount = row[10]
            disbursed_amount = row[11]
            repaid_to_ibrd = row[12]
            due_to_ibrd = row[13]
            exchange_adjustment = row[14]
            borrowers_obligation = row[15]
            sold_3rd_party = row[16]
            repaid_3rd_party = row[17]
            due_3rd_party = row[18]
            loans_held = row[19]
            first_repayment_date = row[20]
            last_repayment_date = row[21]
            effective_date = row[22]
            closed_date = row[23]
            last_disbursed_date = row[24]
            end_of_period = row[25]
            days_to_sign_the_loan = row[26]
            time_taken_for_repayment = row[27]
            gaurantor_country = row[28]
            gaurantor_countrycode = row[29]
            borrower = row[30]
            region = row[31]

            response = dyndb.put_item(
                TableName='loanSt',
                Item={
                'loan_number' : {'S':loan_number},
                'country_code' : {'S':country},
                'country' : {'S':country},
                'loan_type' : {'S':loan_type},
                'loan_status' : {'S':loan_status},
                'interest_rate':{'N':interest_rate},
                'project_id':{'S':project_id},
                'project_name':{'S':project_name},
                'orig_prin_amount':{'N':str(orig_prin_amount)},
                'cancelled_amount':{'N':str(cancelled_amount)},
                'undisbursed_amount':{'N':str(undisbursed_amount)},
                'disbursed_amount':{'N':str(disbursed_amount)},
                'repaid_to_ibrd':{'N':str(repaid_to_ibrd)},
                'due_to_ibrd':{'N':str(due_to_ibrd)},
                'exchange_adjustment':{'N':str(exchange_adjustment)},
                'borrowers_obligation':{'N':str(borrowers_obligation)},
                'sold_3rd_party':{'N':str(sold_3rd_party)},
                'repaid_3rd_party':{'N':str(repaid_3rd_party)},
                'due_3rd_party':{'N':str(due_3rd_party)},
                'loans_held':{'N':str(loans_held)},
                'first_repayment_date':{'S':first_repayment_date},
                'last_repayment_date':{'S':last_repayment_date},
                'effective_date':{'S':effective_date},
                'closed_date':{'S':closed_date},
                'last_disbursed_date':{'S':last_disbursed_date},
                'end_of_period':{'S':end_of_period},
                'days_to_sign_the_loan':{'N':str(days_to_sign_the_loan)},
                'time_taken_for_repayment':{'N':str(time_taken_for_repayment)},
                'gaurantor_country':{'S':gaurantor_country},
                'gaurantor_countrycode':{'S':gaurantor_countrycode},
                'borrower':{'S':borrower},
                'region' : {'S':region},
                }
            )
        print('Put succeeded:')
    except Exception as e:
        print (str(e))
    return {
        'statusCode': 200,
        'body': json.dumps('Data writing done!')
    }