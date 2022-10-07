import copy
import json
import os
import passwords
import pymysql
import re
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


def insertIntoSealingTable(docketnumber, personID, county, total_charges, chargeno, filing_date, disposition_date, dispositioncat, disposition, charge_grade, tenyearfree, tenyearfreedate, OTNViolation, LifetimeConviction, LifetimeConvictionArray, ProhibitedConviction, finesandfees, restitution):
    try:
        connection = pymysql.connect(host=passwords.mysqlip,
                                     user=passwords.mysqlusername,
                                     password=passwords.mysqlpassword,
                                     db=passwords.mysqldb,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        cursor = connection.cursor()

        sql = "INSERT INTO sealing (DocketNumber,PersonID,County,TotalCharges,ChargeNo,FilingDate,DispositionDate,DispositionCategory,Disposition,ChargeGrade,TenYearConvictionFreePeriod,TenYearConvictionFreePeriodEligibleDate,OTNViolation,LifetimeConviction,LifetimeConvictionArray,ProhibitedConviction,FinesAndFees,Restitution) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql, [docketnumber, personID, county, total_charges, chargeno, filing_date, disposition_date, dispositioncat, disposition, charge_grade,
                       tenyearfree, tenyearfreedate, OTNViolation, LifetimeConviction, LifetimeConvictionArray, ProhibitedConviction, finesandfees, restitution])
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
    else:
        return "Unknown"


def checkConvictionRecordXYears(arrest_record, disposition_date, days):
    # For sealing there is only a 10 year window and it's for convictions only
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
    for arrest in arrest_record:
        if(getDispositionCategory(arrest['disposition']) != 'Conviction'):
            # In sealing the subsequent criminal law interaction must be a conviction.
            continue
        elif(arrest['filingDate'] < check_date):
            # Means it happened before the current charge disposition
            continue
        elif(arrest['grade'] == None or not bool(re.match("^M(1|2)|H|F|(^M$)", arrest['grade']))):
            # Only a conviction under M and F will count under 2a1
            continue
        elif(check_date + timedelta(days=days) > compare_date):
            # If we find an arrest record that plus x years is past today than we just take the last record and add x years.
            return ["Not Enough Time", arrest_record[-1]['filingDate'] + timedelta(days=days)]
        elif(check_date + timedelta(days=days) > arrest['filingDate']):
            # We found an arrest within x years
            check_date = arrest['filingDate']
        elif(check_date + timedelta(days=days) < arrest['filingDate']):
            # Means we have a x year window
            return ["True", check_date + timedelta(days=days)]

    # This way if the last arrest date is eligible this should capture it.
    if (check_date + timedelta(days=days) > compare_date):
        return ["Not Enough Time", check_date + timedelta(days=days)]
    else:
        return ["True", check_date + timedelta(days=days)]


def check3BViolation(charges):
    #To check whether there is a conviction for an M1 or higher in the case
    ineligible_grades = ['M1','F','F1','F2','F3','H1','H2','H3']
    for charge in charges:
        if(charge['grade'] != None and getDispositionCategory(charge['disposition']) == 'Conviction' and charge['grade'].upper() in ineligible_grades):
            return True
        # elif(get3a1Category(charge['statuteName'])):
        #     return True

    # This renders a worst-case O(n^2) runtime, but it should be okay 
    # *assuming* that the average cardinality of charges per case are 
    # relatively low (< 100).
    # if check3a2LifetimeConvictions(charges, three_b_call=True)[0]:
    #     return True

    return False



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


def get3a1Category(statute):
    # Prohibited Conviction
    part2_artB = ['18 § 23', '18 § 25', '18 § 26', '18 § 27',
                  '18 § 28', '18 § 29', '18 § 30', '18 § 31', '18 § 32']
    sex_offenses = ('18 § 2902 §§ B', '18 § 2903 §§ B',
                    '18 § 2904', '18 § 2910', '18 § 3124.2 §§ A',
                    '18 § 3126 §§ A1', '18 § 6301 §§ A1ii',
                    '18 § 7507.1', '18 § 1801', '18 § 2252',
                    '18 § 2422 §§ A', '18 § 2423 §§ B', '18 § 2423 §§ C',
                    '18 § 2424', '18 § 2425', '18 § 3011 §§ B',
                    '18 § 3122.1 §§ A2',
                    # (A1-6 and A8 are in tier 1, A7 in tier 3)
                    '18 § 3126 §§ A',
                    '18 § 5902 §§ B', '18 § 5903 §§ A3ii',
                    '18 § 5903 §§ A4ii', '18 § 5903 §§ A5ii', '18 § 5903 §§ A6',
                    '18 § 6312', '18 § 6318', '18 § 6320',
                    '18 § 1591', '18 § 2243', '18 § 2244', '18 § 2251',
                    '18 § 2252 §§ A', '18 § 2260', '18 § 2421', '18 § 2422 §§ B',
                    '18 § 2423 §§ A', '18 § 2901 §§ A1', '18 § 3121',
                    '18 § 3122.1 §§ B', '18 § 3123',
                    '18 § 3125', '18 § 3126 §§ A8',
                    '18 § 4302 §§ B', '18 § 2241', '18 § 2242', '18 § 2244')

    if (statute[0:7] in part2_artB):
        return "Part II Article B"
    elif(statute[0:7] == '18 § 43'):
        return "Part II Article D"
    elif(statute[0:7] == '18 § 61'):
        return "Firearms"
    elif(statute[0:9] == '18 § 5533'):
        return "Cruelty to Animals"
    elif(statute[0:9] == '18 § 6301'):
        return "Corruption of Minors"
    elif(statute.strip().startswith(sex_offenses)):
        return "Sex Offense"
    else:
        return None


def check3a2LifetimeConvictions(full_record, three_b_call=False):

    m1s = 0
    ms = 0
    fs = 0
    specific_lifetime_conviction = False
    indecent_exposure_3127 = False
    sex_with_animal_3129 = False
    fail_register_sex_offender_4915 = False
    inmate_weapon_5122 = False
    paramilitary_training_5515 = False
    abuse_of_a_corpse_5510 = False

    for charge in full_record:
        # print(charge['grade'],getDispositionCategory(charge['disposition']))
        if(getDispositionCategory(charge['disposition']) != 'Conviction'):
            continue
        elif(charge['grade'] != None and charge['grade'].upper().startswith(('F', 'H'))):
            fs += 1
        elif (charge['grade'] != None and charge['grade'].upper() == 'M1'):
            m1s += 1
            ms += 1
        elif (charge['grade'] != None and
              charge['grade'].upper().startswith('M') and not charge['grade'].upper().startswith('M3')):
            ms += 1

        if(len(charge['statuteName']) >= 9 and charge['statuteName'][0:9] == '18 § 5510'):
            # Abuse of a Corpse
            abuse_of_a_corpse_5510 = True
            specific_lifetime_conviction = True
        elif(len(charge['statuteName']) >= 9 and charge['statuteName'][0:9] == '18 § 5515'):
            # Paramilitary Training 55
            paramilitary_training_5515 = True
            specific_lifetime_conviction = True
        elif(len(charge['statuteName']) >= 9 and charge['statuteName'][0:9] == '18 § 5122'):
            # Inmate with Weapon 5122
            inmate_weapon_5122 = True
            specific_lifetime_conviction = True
        elif(len(charge['statuteName']) >= 14 and (charge['statuteName'] == '18 § 4915 §§ A1' or charge['statuteName'] == '18 § 4915 §§ A2')):
            # Fail to register sex offender 4915
            fail_register_sex_offender_4915 = True
            specific_lifetime_conviction = True
        elif(len(charge['statuteName']) >= 9 and charge['statuteName'][0:9] == '18 § 3129'):
            # Sex with Animal 3129
            sex_with_animal_3129 = True
            specific_lifetime_conviction = True
        elif(len(charge['statuteName']) >= 9 and charge['statuteName'][0:9] == '18 § 3127'):
            # Indecent Exposure 3127
            indecent_exposure_3127 = True
            specific_lifetime_conviction = True

    # If 3a2 checks are called within 3b, then the aggregate 
    # number of charges is irrelevant
    if three_b_call:
        ms = fs = m1s = -1

    json_array = {
        "Ms": ms,
        "Fs": fs,
        "M1s": m1s,
        "specific_lifetime_conviction": specific_lifetime_conviction,
        "indecent_exposure_3127": indecent_exposure_3127,
        "sex_with_animal_3129": sex_with_animal_3129,
        "fail_register_sex_offender_4915": fail_register_sex_offender_4915,
        "inmate_weapon_5122": inmate_weapon_5122,
        "paramilitary_training_5515": paramilitary_training_5515,
        "abuse_of_a_corpse_5510": abuse_of_a_corpse_5510
    }
    # Check to see if we reached any of the thresholds
    if(ms >= 4 or fs >= 1 or m1s >= 2 or specific_lifetime_conviction == True):
        # The first True means there is a violation
        return [True, json.dumps(json_array)]
    else:
        return [False, json.dumps(json_array)]


def runDocket(county, docketNumber):
    sql = "SELECT * FROM " + county + "_cases as cases INNER JOIN " + county + \
        "_charges as charges on cases.docketNumber = charges.docketNumber WHERE cases.docketNumber = %s"
    charges = runSelectQuery(sql, [docketNumber])
    if(not charges):
        return False
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

    full_record = getPersonsRecord(county, charges[0]['defendantFirstName'], charges[0]
                                   ['defendantLastName'], charges[0]['defendantGender'], charges[0]['defendantDOB'])
    fines_fees = charges[0]['currentBalance']
    restitution = charges[0]['restitutionBalance']
    filing_date = charges[0]['filingDate']
    liftime_convictions = check3a2LifetimeConvictions(full_record)

    # This is a hacky workaround if the disposition date is None we make it equal to the filing date  which will be close enough for our purposes.
    otn_violation = check3BViolation(charges)

    disposition_date = charges[0]['dispositionDate'] if charges[0]['dispositionDate'] != None else charges[0]['filingDate']
    for charge in charges:
        # print(charge)

        ten_years = checkConvictionRecordXYears(
            full_record, disposition_date, 3650)

        disposition = charge['disposition'].lower(
        ) if charge['disposition'] != None else charge['disposition']
        charge_status = charge['status'].lower(
        ) if charge['status'] != None else charge['status']
        # This is a hacky workaround to make sure we don't get an out of range error.
        staute_with_spaces = " " * \
            15 if charge['statuteName'] == None else charge['statuteName'] + \
            (" " * 15)
        prohibited_conviction = get3a1Category(staute_with_spaces)
        dispositionCategory = getDispositionCategory(disposition)
        
        charge_grade = charge['grade'].upper(
        ) if charge['grade'] != None else charge['grade']
        insertIntoSealingTable(docketNumber, personID, county, total_charges, charge['sequenceNumber'], filing_date, disposition_date, dispositionCategory, disposition,
                               charge_grade, ten_years[0], ten_years[1], otn_violation, liftime_convictions[0], liftime_convictions[1], prohibited_conviction, fines_fees, restitution)


def start(county):
    print("Starting with ", county)
    sql = "SELECT docketNumber FROM " + county + \
        "_cases WHERE status in ('Closed','Adjudicated','Adjudicated/Closed') AND docketNumber NOT IN (SELECT DISTINCT(DocketNumber) FROM sealing) LIMIT %s"
    cases = runSelectQuery(sql, [999999999])
    for x, docket_number in enumerate(cases):
        print(x, ":", docket_number['docketNumber'])
        runDocket(county, docket_number['docketNumber'])

if __name__ == '__main__':
    # runDocket(county,'CP-10-CR-0001051-2008')
    start('lawrence')
    start('butler')
    start('beaver')
    start('allegheny')
