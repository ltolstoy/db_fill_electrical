#!/home/ltolstoy/anaconda3/bin/python3.6
"""
Script to read all xxx_electrical.csv files for sites, then create a record and put it into db
Without re-processinf of csv, but with creating additional columns as necessary, changing time format, addin Pdiss, etc

"""

import glob, pickle
import fnmatch, os, sys
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import io  #cstringio is gone in Python 3+  !


def get_list_of_files_in_folder(pat, folder):
    '''
        Looks for files matching the pattern '*_electrical.csv' in specific folder in path
        '/media/ltolstoy/My Passport/dataservices/data_log_older_files_folder/**/'
        pat - '201708' ex, to select only month of data
        :return: List of found files with full path
        '''
    matches = []  # list of all found xxx_electrical.csv files recursively in the folder
    total_size = 0  # count total size of found files
    #path = '/media/ltolstoy/My Passport/dataservices/data_log_older_files_' + folder + '/**/'
    if os.path.exists('/mnt/data_log/' + folder):
        path = '/mnt/data_log/' + folder + '/**/'
        for file in glob.glob(path + pat + '*_electrical.csv',
                              recursive=True):
            # file = os.path.join(froot, filename)  # full path to current xml
            total_size += os.path.getsize(file)
            matches.append(file)  # Now we have the list of all csv files
        matches.sort(reverse=False)  # to sort from the  oldest to the newest 
        # matches.sort()
    else:
        print("Path {} does not exist! Wrong name in the list?".format('/mnt/data_log/' + folder))
    return matches, total_size


def get_list_of_files(pat):
    '''
    Looks for files matching the pattern '*_electrical.csv'
    pat - '201708' ex, to select only month of data
    :return: List of found files with full path
    '''
    matches = []  # list of all found xxx_electrical.csv files recursively in the folder

    for file in glob.glob(
                            '/mnt/data_log/enerparc/**/' + pat + '*_electrical.csv',
                            recursive=True):
        # file = os.path.join(froot, filename)  # full path to current xml
        matches.append(file)  # Now we have the list of all csv files
    matches.sort(reverse=True)  # to sort from the newest to the oldest
    # matches.sort()
    return matches


def check_header(h):
    """ Checks if header of csv file is what we expected, as there were changes in format
    h - list like ['Mac', 'SN', 'Time', 'Date', 'Date_Time', 'Location', 'Vin1', 'Vin2', 'Vout',
    'Iin1', 'Iin2', 'Iout', 'Text', 'Pdiss', 'Pout']
    return 0 (not correct format), or for cases
     b'Mac,SN,Time,Date,Date_Time,Location,Vin1,Vin2,Vout,Iin1,Iin2,Iout,Text,Pdiss,Pout\r\n',   1
     b'Mac,date_time,Vin1,Vin2,Vout,Iin1,Iin2,Iout,Text,Pdiss\r\n',                              4
     b'Mac,SN,Time,Date,Date&Time,Location,Vin1,Vin2,Vout,Iin1,Iin2,Iout,Text,Pdiss,Pout\r\n',   1
     b'Mac,date_time,Vin1,Vin2,Vout,Iin1,Iin2,Iout,Text\r\n',                                    3
     b'Mac,SN,Time,Date,Location,Vin1,Vin2,Vout,Iin1,Iin2,Iout,Text,Pdiss,Pout\r\n'              2
"""
    if (len(h) == 15 and
                h[0] == 'Mac' and h[1] == 'SN' and
                h[2] == 'Time' and h[3] == 'Date' and
            (h[4] == 'Date_Time' or h[4] == 'Date&Time') and h[5] == 'Location' and
                h[6] == 'Vin1' and h[7] == 'Vin2' and
                h[8] == 'Vout' and h[9] == 'Iin1' and
                h[10] == 'Iin2' and h[11] == 'Iout' and
                h[12] == 'Text' and h[13] == 'Pdiss' and
                h[14] == 'Pout'):
        return 1
    elif (len(h) == 14 and
                  h[0] == 'Mac' and h[1] == 'SN' and
                  h[2] == 'Time' and h[3] == 'Date' and
                  h[4] == 'Location' and
                  h[5] == 'Vin1' and h[6] == 'Vin2' and
                  h[7] == 'Vout' and h[8] == 'Iin1' and
                  h[9] == 'Iin2' and h[10] == 'Iout' and
                  h[11] == 'Text' and h[12] == 'Pdiss' and
                  h[13] == 'Pout'):
        return 2
    elif (len(h) == 9 and
                  h[0] == 'Mac' and h[1] == 'date_time' and
                  h[2] == 'Vin1' and h[3] == 'Vin2' and
                  h[4] == 'Vout' and h[5] == 'Iin1' and
                  h[6] == 'Iin2' and h[7] == 'Iout' and
                  h[8] == 'Text'):
        return 3
    elif (len(h) == 10 and
                  h[0] == 'Mac' and h[1] == 'date_time' and
                  h[2] == 'Vin1' and h[3] == 'Vin2' and
                  h[4] == 'Vout' and h[5] == 'Iin1' and
                  h[6] == 'Iin2' and h[7] == 'Iout' and
                  h[8] == 'Text' and h[9] == 'Pdiss'):
        return 4
    else:
        return 0  # means not recognized format of the header


def mac2ser(mac):
    """
    Converts from mac to serial. Adapted from Corey's script mac2serial
    :param mac: string
    :return: serial number as string
    """
    try:
        mac = str(mac)  # make sure that type(mac) is not int, as int can't be 'upper()'
        mac = mac.upper()
        imac = mac[:6]
        number = mac[6:]
        week = '%02d' % ((int(imac, 16) >> 18) & 0x3f)
        year = '%02d' % ((int(imac, 16) >> 11) & 0x7f)
        letter = chr(((int(imac, 16) >> 6) & 0x1F) + 65)
        ser = '%06d' % int(number, 16)
        serial = week + year + letter + ser
        return serial
    except Exception as exc:
        print("mac2serial: got {} as input, can't convert to serial".format(mac))
        print(exc)
        return ""


def dict_from_file():
    # Loads previously ceated dict from pickled file '/home/ltolstoy/Downloads/scripts/db_filling/all_pickled'
    # which was creted from many structures_xxx.xml by joined_structures_v2.py
    # format: key:mac value:(sn, sku, [loc], ts, ch, gw, ed, [filename list])
    dict = pickle.load(open('/home/ltolstoy/scripts/joined_structures/all_sites_pickled_v6', 'rb'))

    return dict


def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def put_data_into_db(p2f):
    """ Puts data from the file into db
    p2f - full path to csv file, '/home/ltolstoy/Downloads/data_logs/canadian_solar/1707/20170701_301_electrical.csv'
    header for recent files would be
    ['Mac', 'SN', 'Time', 'Date', 'Date_Time', 'Location', 'Vin1', 'Vin2', 'Vout', 'Iin1', 'Iin2', 'Iout', 'Text', 'Pdiss', 'Pout']
    Output: number of lines entered into db (count dataframe)
    """
    all = dict_from_file()  # all - is a dict with all macs found in canadian_solar xmls with corresponding data
    tmp = os.path.split(p2f)[1]  # to get block name like '301' from full path name '/home/ltolstoy/Downloads/data_logs/canadian_solar/1707/20170701_301_electrical.csv'
    block = tmp[tmp.find('_b',7) + 1:tmp.find('_electrical')]  #extract 'b308_1' from '20171031_b308_1_electrical.csv'
    date1 = os.path.split(p2f)[1][:8]  # to get date like 20170701 from full path name
    date = date1[:4] + '-' + date1[4:6] + '-' + date1[6:]  # to get date like '2017-07-01' from 20170701
    site = os.path.split(p2f)[0].split('/')[3]  # to get "aikawa"
    site_name = site + "_" + block  # to get "canadian_solar_xxx" and put it into SITE column in db
    flag = 0  # remove file if all db processing was fine, keep it otherwise
    num_lines = file_len(p2f)  # count all lines in file

    if num_lines > 1:
        with open(p2f, "rb") as infile:
            df = pd.read_csv(infile, dtype={'Mac': object}, low_memory=False)  # read Mac as object (str), not np.float64 as happened for mac 7072800002E7
            df['Mac'] = df['Mac'].astype(str)  # making sure Mac is a string, not int or np.float64

            header = list(df.columns.values)  # get list of column names
            if check_header(header) == 1:  # means header is current, we can proceed
                # Mac,SN,Time,Date,Date_Time,Location,Vin1,Vin2,Vout,Iin1,Iin2,Iout,Text,Pdiss,Pout
                if 'Date_Time' in df.columns:
                    df.drop(['Date_Time'], axis=1, inplace=True)  # Drop Date_Time column
                elif 'Date&Time' in df.columns:
                    df.drop(['Date&Time'], axis=1, inplace=True)  # Drop Date_Time column
                df.insert(4, "Site",
                          site_name)  # insert new column at loc 4 (before Location), with name "Site", and value the same for all raws
            elif check_header(header) == 2:
                # Mac,SN,Time,Date,Location,Vin1,Vin2,Vout,Iin1,Iin2,Iout,Text,Pdiss,Pout     14 elem
                df.insert(4, "Site", site_name)

            elif check_header(header) == 3:
                # Mac,date_time,Vin1,Vin2,Vout,Iin1,Iin2,Iout,Text - 9 elems
                # Problem that here is no really date and sn: 308280000027,04:31:35,431.76,518.04,525.7,0.008,0.003,0.0,0.91,5.008
                df.insert(1, "SN", '')  # fill all with the same SN obtained from corresponding MAC
                df['SN'] = df.apply(lambda row: mac2ser(row['Mac']), axis=1)

                df.rename(columns={'date_time': 'Time'}, inplace=True)
                df.insert(3, "Date", date)
                df.insert(4, "Site", site_name)
                df.insert(5, "Location", '')
                df['Location'] = df.apply(lambda row: all[row['Mac']][3][-1] if row['Mac'] in all else '',
                                          axis=1)  # put corresp loc,but exclude 1st symbol so '405.02.10-8'- > '05.02.10-8'

                df.insert(13, "Pdiss", df['Vin1'] * df['Iin1'] + df['Vin2'] * df['Iin2'] - df['Vout'] * df['Iout'])
                df.insert(14, "Pout", df['Vout'] * df['Iout'])

            elif check_header(header) == 4:
                # Mac,date_time,Vin1,Vin2,Vout,Iin1,Iin2,Iout,Text,Pdiss  10 elements
                #  here is no really date and sn: 308280000027,04:31:35,431.76,518.04,525.7,0.008,0.003,0.0,0.91,5.008
                df.insert(1, "SN", '')  # fill all with the same SN obtained from corresponding MAC
                df['SN'] = df.apply(lambda row: mac2ser(row['Mac']), axis=1)
                df.rename(columns={'date_time': 'Time'}, inplace=True)
                df.insert(3, "Date", date)
                df.insert(4, "Site", site_name)
                df.insert(5, "Location", '')
                df['Location'] = df.apply(lambda row: all[row['Mac']][3][-1] if row['Mac'] in all else '',
                                          axis=1)  # put corresp location string from 'all',but exclude 1st symbol so '405.02.10-8'- > '05.02.10-8'

                df.insert(14, "Pout", df['Vout'] * df['Iout'])
            else:
                print("File {} has incompatible header, cant process it yet.".format(p2f))
                return 0  # to exit the function but continue with next file
                # to exit the loop and skip insertion

        df.columns = map(str.lower,
                         df.columns)  # need to rename column names to lower case, as Postgresql normalizes ALL column nmaes to lower case!
        
        address = 'postgresql://ltolstoy:PWD@172.16.248.141:5432/electrical'    #new location for DbServer
        engine = create_engine(address)
        connection = engine.raw_connection()
        cursor = connection.cursor()
        output = io.StringIO()  # stream the data using 'to_csv' and StringIO(); then use sql's 'copy_from' function
        df.to_csv(output, header=False,
                  index=False)  
        output.seek(0)  # jump to start of stream

        try:
            pass
            cursor.copy_from(output, 'data_electrical_2018', sep=",", null="")  # file_from , table_name in db, searator, encoding
            #connection.commit()
            cursor.close()
            return len(df.index)        #counting number of lines entered
        except Exception as inst:
            print("Error in writing dataframe to database, for file {}".format(p2f))
            print(type(inst))
            print(inst.args)
            print(inst)
            flag = 0  # to prevent deletion
            os.system('spd-say "Leo, your running program has raised an exception"')
            return 0

    else:  # num_lines = 1, just header
        print("File {} has {} line, too small, just skipping it now".format(p2f, str(num_lines)))
        # os.remove(p2f)
        return 0
    # time to remove processed file
    if flag == 1:
        # os.remove(p2f)     #don't remove from My passport!
        pass


def main():
    """
    Finds all files (for a month), and process them 1-by-1 by checking csv header, 
    adding column Site, deleting Date_Time,
    and then puting it into db
    :return: nothing
    """
    
    lof = [ 'aikawa', 'canadian_solar',
           'copia_fuse', 'elgris', 'enerparc', 'farmdo_mongolia', 'fuji', 'gandn', 
            'gandn_kamiizumi', 'higashi','hiji_yurino','itf', 'kacobp50test', 'kee',
            'miraclehill', 'mo_katori', 'msf', 'nano', 'planeko',
           'rbi', 'refu', 'sonnen', 'syncarpha', 'xsol_yachimata']  # list of folders in "/mnt/data_log/xxxx" to copy data from
    
    #lof = ['omron']        #for testing
    c_f = 1  # counter for processed folders
    t_size = 0          #total size added
    t_lines = 0         #total lines added
    s00 = datetime.now()
    for folder in lof:
        list_of_files, size_in_site = get_list_of_files_in_folder('201804',
                                                                  folder)  # for 1 month - getting list of month found xxx_electrical.csv files, recursively

        num_f = len(list_of_files)
        s0 = datetime.now()  # start time, in datetime format
        # print("")
        print(("Beginning processing not compressed files, current time {} ,"
               " found {} files to process in folder {} which is {}/{}, data size is {}Gb").format(
            str(datetime.now()).split('.')[0],
            num_f, folder, c_f, len(lof), round(size_in_site / 1e9, 2)))
        c = 1  # count files
        #list_of_files=['/mnt/data_log/gandn/180325/20180325_b11_electrical.csv']    #for debugging
        for f in list_of_files:
            s1 = datetime.now()
            print("--- {} , working on file {}".format(str(datetime.now()).split('.')[0], f))
            lc = put_data_into_db(f)        # inserted lines counter
            t_lines += lc       # add all processed lines
            s2 = datetime.now()
            #td = s2 - s1  # this is timedelta object
            print(("Processed {}/{} or {}%, took {} or {} from start,"
                   " on average {} per file, ETF folder {}").format(c, num_f,
                    round(c * 100 / num_f, 2),
                    str(s2 - s1).split('.', 2)[0], str(s2 - s00).split('.', 2)[0],
                    str((s2 - s0) / c).split(':', 1)[1].split('.')[0],
                    str((num_f - c) * (s2 - s0) / c).split('.')[0]))

            c += 1
        c_f += 1  # count folders processed
        t_size += size_in_site  # count total processed size in all folders

    se = datetime.now()  # last time
    td1 = se - s00
    print(("End of work, current time {}. All taken time is {},"
           " processed {}Gb and {} lines").format(str(datetime.now()).split('.')[0],
                  str(td1).split('.', 2)[0],
                  round(t_size / 1e9, 2),
                  t_lines))
    os.system('spd-say "Leo, your fill electrical database program has finished"')


if __name__ == '__main__': main()
