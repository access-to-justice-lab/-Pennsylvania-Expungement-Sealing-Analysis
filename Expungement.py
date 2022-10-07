import copy
import os
import passwords
import pymysql
import sys
from constants import COMPARE_YEAR, COMPARE_MONTH, COMPARE_DAY
from datetime import timedelta
from datetime import date


def runSelectQuery(sql, arguments, trynumber=0):
    try:
        connection = pymysql.connect(host=passwords.mysqlip,
                                     user=passwords.mysqlusername,
                                     password=passwords.mysqlpassword,
                                     db=passwords.mysqldb,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        cursor = connection.cursor()
        cursor.execute(sql, arguments)
        results = cursor.fetchall()
        cursor.close()
        if(len(results) == 0):
            return None
        else:
            return results
    except Exception as e:
        print("Error")
        print(arguments)
        print(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print("Try Number", trynumber)
        if(e.args[0] == 2003 or e.args[0] == 2013):
            # weird Name resolution issue or sql lost connection error
            if(trynumber == 10):
                # Ok we're giving up
                f = open("name_resolution.txt", "a")
                if (len(arguments) > 1):
                    f.write("\r\n" + arguments[0] + " " + arguments[1])
                else:
                    f.write("\r\n" + arguments[0])
                f.close()
                return None
            else:
                trynumber += 1
                runSelectQuery(sql, arguments, trynumber)
        else:
            sys.exit(1)


def insertIntoExpungementTable(docketnumber, personID, county, total_charges, chargeno, charge_id, filing_date, dispositioncat, charge_grade, fiveyearfree, fiveyearfreedate, ardcompletion, age, tenyearfree, tenyearfreedate, finesandfees, restitution):
    try:
        connection = pymysql.connect(host=passwords.mysqlip,
                                     user=passwords.mysqlusername,
                                     password=passwords.mysqlpassword,
                                     db=passwords.mysqldb,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        cursor = connection.cursor()

        sql = "INSERT INTO expungement (DocketNumber, PersonID,County,TotalCharges,ChargeNo,ChargeID,FilingDate,DispositionCategory,ChargeGrade,FiveYearFreePeriod,FiveYearFreePeriodEligibleDate,ARDCompletion,Age,TenYearArrestFreePeriod,TenYearArrestFreePeriodEligibleDate,FinesAndFees,Restitution) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql, [docketnumber, personID, county, total_charges, chargeno, charge_id, filing_date, dispositioncat,
                       charge_grade, fiveyearfree, fiveyearfreedate, ardcompletion, age, tenyearfree, tenyearfreedate, finesandfees, restitution])
        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        cursor.close()
        connection.close()
        print(e)
        print(type(e))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        if(e.args[0] == 1062):
            # Duplicate Entry
            print("Duplicate")
            return None
        else:
            print("Unknown Error")
            sys.exit(1)



def getDispositionCategory(disposition):
    disposition = disposition.lower() if disposition != None else None
    non_conviction_strings = ["nolle", "dismiss", "quashed"]

    if disposition == None:
        return "Unknown"
    elif (any(x in disposition for x in non_conviction_strings) or disposition.startswith(("not guilty", "withdrawn", "judgment of acquittal", "mistrial", "demurrer sustained"))):
        return "Non-Conviction"
    elif (len(disposition) >= 6 and disposition.startswith(("guilty", "nolo contendere"))):
        return "Conviction"
    # Removed ARD from the equation as this disposition was just too confusing to determine eligibiltiy for. A later analysis could try to add it in.
    # elif('ard' in disposition):
    #     return "ARD"
    else:
        return "Unknown"


def checkARDEligibilty(disposition_category, case_status):
    # If the case status is active we consider it pending otherwise we consdier it closed.
    case_status = case_status.lower() if case_status != None else None

    if (disposition_category == "ARD" and (case_status == 'closed' or case_status == 'adjudicated')):
        return "ARD Completed"
    elif (disposition_category == "ARD" and case_status != 'closed' and case_status != 'adjudicated'):
        return "ARD Pending"
    else:
        return "No"


def calculateAge(dob):
    if (dob == None):
        # If we don't have a DOB we assume they are younger than 70 which is the more stringent standard.
        return None
    # We need to set a date so we can replicate.
    today = date(COMPARE_YEAR, COMPARE_MONTH, COMPARE_DAY)
    age = today.year - dob.year - \
        ((today.month, today.day) < (dob.month, dob.day))
    return age


def getPersonsRecord(county, firstname, lastname, gender, dob):
    # Pull all the filing dates for a person.
    # There's got to be a better way than all of the if/else statements
    sql = "SELECT * FROM " + county + "_cases as cases INNER JOIN " + county + \
        "_charges as charges ON cases.docketNumber = charges.docketNumber WHERE "
    sql_variable_array = []
    if(firstname != None):
        sql += "defendantFirstName= %s AND "
        sql_variable_array.append(firstname)
    else:
        sql += "defendantFirstName IS NULL AND "

    if(lastname != None):
        sql += "defendantLastName= %s AND "
        sql_variable_array.append(lastname)
    else:
        sql += "defendantLastName IS NULL AND "

    if(gender != None):
        sql += "defendantGender= %s AND "
        sql_variable_array.append(gender)
    else:
        sql += "defendantGender IS NULL AND "

    if(dob != None):
        sql += "defendantDOB= %s "
        sql_variable_array.append(dob)
    else:
        sql += "defendantDOB IS NULL "

    sql += "ORDER BY filingDate ASC"

    arrest_record = runSelectQuery(sql, sql_variable_array)
    return arrest_record


def checkArrestRecordXYears(arrest_record, disposition_date, days):
    # arrest_record = copy.copy(arrest_record_original)
    # arrest_record.pop()
    # print("Arrest Record",days,arrest_record)
    # The check date starts as he disposition date and then becomes a subsequent arrest if one is found.
    check_date = disposition_date
    compare_date = date(COMPARE_YEAR, COMPARE_MONTH, COMPARE_DAY)
    if(arrest_record == None or len(arrest_record) == 0):
        # Means there is no arrest record
        if (check_date + timedelta(days=days) > compare_date):
            return ["Not Enough Time", check_date + timedelta(days=days)]
        else:
            return ["True", check_date + timedelta(days=days)]
    # Means we do have an arrest record.
    for arrest_date in arrest_record:
        if(arrest_date['filingDate'] < check_date):
            # Means it happened before the current charge disposition
            continue
        if(check_date + timedelta(days=days) > compare_date):
            # If we find an arrest record that plus x years is past today than we just take the last record and add x years.
            return ["Not Enough Time", arrest_record[-1]['filingDate'] + timedelta(days=days)]
        elif(check_date + timedelta(days=days) > arrest_date['filingDate']):
            # We found an arrest within x years
            check_date = arrest_date['filingDate']
        elif(check_date + timedelta(days=days) < arrest_date['filingDate']):
            # Means we have a x year window
            return ["True", check_date + timedelta(days=days)]

    # This way if the last arrest date is eligible this should capture it.
    if (check_date + timedelta(days=days) > compare_date):
        return ["Not Enough Time", check_date + timedelta(days=days)]
    else:
        return ["True", check_date + timedelta(days=days)]


def runDocket(county, docketNumber):
    sql = "SELECT *,charges.ID as charges_id FROM " + county + "_cases as cases INNER JOIN " + county + \
        "_charges as charges on cases.docketNumber = charges.docketNumber WHERE cases.docketNumber = %s"
    charges = runSelectQuery(sql, [docketNumber])
    # print(charges[0])
    if(not charges):
        return False
    full_record = getPersonsRecord(county, charges[0]['defendantFirstName'], charges[0]
                                   ['defendantLastName'], charges[0]['defendantGender'], charges[0]['defendantDOB'])

    # Get the total number of charges
    total_charges = len(charges)

    # Create the Unique person ID by combining the name, dob, and gender. This is what we use to search for other records.
    personID = charges[0]['defendantFirstName'] if charges[0]['defendantFirstName'] != None else "_NONE"
    personID += "_" + \
        charges[0]['defendantLastName'] if charges[0]['defendantLastName'] != None else "_NONE"
    personID += "_" + charges[0]['defendantDOB'].strftime(
        "%m-%d-%Y") if charges[0]['defendantDOB'] != None else "_NONE"
    personID += "_" + \
        charges[0]['defendantGender'] if charges[0]['defendantGender'] != None else "_NONE"

    age = calculateAge(charges[0]['defendantDOB'])
    fines_fees = charges[0]['currentBalance']
    restitution = charges[0]['restitutionBalance']
    filing_date = charges[0]['filingDate']
    for charge in charges:
        # print(charge)
        if(charge['dispositionDate'] == None):
            # We make the disposition date equal to the filing date. This is not perfect but close enough given the data limitations.
            five_years = checkArrestRecordXYears(
                full_record, charge['filingDate'], 1825)
            ten_years = checkArrestRecordXYears(
                full_record, charge['filingDate'], 3650)
        else:
            # This is the normal scenario where we have a disposition date
            five_years = checkArrestRecordXYears(
                full_record, charge['dispositionDate'], 1825)
            ten_years = checkArrestRecordXYears(
                full_record, charge['dispositionDate'], 3650)

        disposition = charge['disposition'].lower(
        ) if charge['disposition'] != None else charge['disposition']
        charge_status = charge['status'].lower(
        ) if charge['status'] != None else charge['status']

        dispositionCategory = getDispositionCategory(disposition)
        charge_grade = charge['grade'].upper(
        ) if charge['grade'] != None else charge['grade']
        ard_eligibility = checkARDEligibilty(
            dispositionCategory, charge_status)

        insertIntoExpungementTable(docketNumber, personID, county, total_charges, charge['sequenceNumber'], charge['charges_id'], filing_date, dispositionCategory, charge_grade, five_years[0], five_years[1],
                                   ard_eligibility, age, ten_years[0], ten_years[1], fines_fees, restitution)


def start(county):
    # runDocket(county,'CP-37-CR-0001245-2010')
    sql = "SELECT docketNumber FROM " + county + \
        "_cases WHERE status in ('Closed','Adjudicated','Adjudicated/Closed') LIMIT %s"
    cases = runSelectQuery(sql, [999999999])
    for x, docket_number in enumerate(cases):
        print(x, ":", docket_number['docketNumber'])
        runDocket(county, docket_number['docketNumber'])

if __name__ == '__main__':
    start('lawrence')
    start('butler')
    start('beaver')
    start('allegheny')
