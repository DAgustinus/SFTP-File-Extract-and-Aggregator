import os
import zipfile
import pandas as pd
import numpy as np
import os.path
import sys
import xlsxwriter
import shutil
import codecs

std_header = ['Date','Search Engines','Search Keywords','Mobile Device Type','Mobile Devices','Entry Pages','Visits','Page Views']
conv_name = []
ro_name = []

file_list = []
kwp_date = []


ftp_username = input('Username: \n')
ftp_pw = input('Password: \n')
ftp_acc_id = input('Account ID: \n')
directory = "/PR/" + ftp_acc_id
start = input('Start date: YYYYMMDD \n')
end = input('End date: YYYYMMDD \n')
get_file_name = input('What would you like to name your file: \n')
file_save = input('Save Adobe Raw files in the computer: Y/N\n')
file_save = file_save.lower()
save_pages = input('Save detail entry pages: Y/N\n')
save_pages = save_pages.lower()


from ftplib import FTP
ftp = FTP('sftp.brightedge.com')
ftp.login(ftp_username,ftp_pw)
# print('List of files')
ftp.cwd(directory)
# ftp.retrlines('LIST')
print("You have accessed",directory,"folder!")



pnd_dl_list = []
pnkd_dl_list = []
ptd_dl_list = []
file_KWPage = []

filtered_pnd = []
filtered_pnkd = []
filtered_ptd = []
fl_KWPage = []

# Get list of files in an array and total amount (Complete)
start_int = int(start)
print('Collecting data from:',start_int,'TO',end)
end_int = int(end) + 1
num = start_int
# print(date_list + end_int)

st = 'page_name_daily_'
su = 'page_name_keyword_daily_'
sx = 'page_type_daily_'
lists = []

#### working download ftp ####
file_list_pnd = ftp.nlst('page_name_daily_*')
total_pnd_files = len(file_list_pnd)

file_list_pnkd = ftp.nlst('page_name_keyword_daily_*')
total_pnkd_files = len(file_list_pnkd)

file_list_ptd = ftp.nlst('page_type_daily_*')
total_ptd_files = len(file_list_ptd)

kwp = 'BE_SearchEngineKWPageName_' + str(ftp_acc_id) +'_*'

file_KWPage = ftp.nlst(kwp)


 
# Get total files (Complete)
total_all_files = len(file_list_pnd) + len(file_list_pnkd) + len(file_list_ptd)

# print("There are ", total_all_files, " in total")

print('###################')
print('Starting process...')
print('###################')

# Function  that takes extract (Complete)
def file_extract( str ):
    filename = str
    print('Opening local file ' + filename)
    file = open(filename, 'wb')

    print('Getting ' + filename)
    ftp.retrbinary('RETR %s' % filename, file.write)
    # Clean up time
    print('Closing file ' + filename)
    file.close()
    zip_ref = zipfile.ZipFile(str)
    zip_ref.extractall()  

def file3_dl(startIt,endIt):
    S = int(startIt)
    E = int(endIt)
    Q = S
    while Q <= E:
        if Q == 20161232:
           Q = 20170101
        kwp_date.append(str(Q))
        fl = 'BE_SearchEngineKWPageName_' + str(ftp_acc_id)+ '_'+ str(Q)
        print(fl)
        matching = [s for s in file_KWPage if fl in s]
        if len(matching) > 0:
            print('Downloading ', matching)
            fl_KWPage.append(matching[0])
            Q = Q + 1
        else:
            Q = Q + 1
    if len(matching) < 1:
        print('none')
        Q = Q + 1
    else:
        print('Downloading')
        filename = matching[0]
        file = open(filename, 'wb')
        ftp.retrbinary('RETR %s' % filename, file.write)
        Q = Q + 1
                    
# While loop to extract the files and keep track on how many have we downloaded
def file_download(int,arr):
    # while counter (Complete)
    count = 0
    dl_count = 0
    # downloaded counter (Complete)
    while (count < int):
        print('---------------------------------')
        print('Attempting to extract ',arr[count])      #file_list[count]
        file_extract(arr[count])                        #file_list[count]
        count = count +1
        dl_count = dl_count + 1


file3_dl(start,end)
total_kwp_dl = len(fl_KWPage)
file_download(total_kwp_dl, fl_KWPage)

print('##########################################################')

############### END OF EXTRACTOR AND UNZIPPER #################
first_location = os.getcwd()
new_location = os.getcwd()
dirs = os.listdir(new_location)

kwp_d = []
kwp_v = []
kwp_pv = []
kwp_o = []
kwp_r = []
kwp_conv = []
kwp_date = []
kwp_se = []
kwp_ep = []
kwp_allPages = []


def createIndex(fname):
    toUTF8 = codecs.getencoder('UTF8')

#### Function defined to read each files and get sum
def get_pages(str):
    df = pd.read_csv(str, encoding = "ISO-8859-1")
    date = df["Date"]
    se = df["Search Engines"]
    ep = df["Entry Pages"]
    vi = df['Visits']
    pv = df['Page Views']
    mt = df['Mobile Device Type']
    df_out = pd.DataFrame({'Date': date,
                           'Search Engines': se,
                           'Mobile Device Type': mt,
                           'Entry Pages': ep,
                           'Visits': vi,
                           'Page Views': pv})
    return df_out
    
def get_date(str):
    df = pd.read_csv(str, encoding = "ISO-8859-1")
    total = df["Date"]
    return total

def get_se(str):
    df = pd.read_csv(str, encoding = "ISO-8859-1")
    total = df["Search Engines"]
    return total

def get_ep(str):
    df = pd.read_csv(str, encoding = "ISO-8859-1")
    total = df["Entry Pages"]
    return total

def get_sum(str):
    df = pd.read_csv(str, encoding = "ISO-8859-1")
    total_sum_v = df["Visits"].sum()
    return total_sum_v

def get_orders(str):
    df = pd.read_csv(str, encoding = "ISO-8859-1")
    total_sum_v = df["Orders"].sum()
    # print(total_sum_v) #For QA
    return total_sum_v

def get_revenue(str):
    df = pd.read_csv(str, encoding = "ISO-8859-1")
    total_sum_v = df["Revenue"].sum()
    # print(total_sum_v) #For QA
    return total_sum_v

def get_page_views(str):
    df = pd.read_csv(str, encoding = "ISO-8859-1")
    total_sum_pv = df["Page Views"].sum()
    return total_sum_pv

def get_conv(fl_name,conv):
    df = pd.read_csv(fl_name, encoding = "ISO-8859-1")
    total_sum = df[conv].sum()
    return total_sum

def get_headers(fl_name):
    df = pd.read_csv(fl_name, encoding = "ISO-8859-1")
    header_list = df.columns
    return header_list

#########
for file in dirs:
    if file.find("BE_SearchEngine") != -1:
        if file.find(".csv") != -1:
            head_list = get_headers(file)
            list_len = len(head_list)
            break

#########

# print(head_list)    #For QA
# print(list_len)     #For QA

def get_headers(fl_name):
    df = pd.read_excel(fl_name)
    header_list = df.columns
    return header_list

tf_order = False
tf_rev = False

if list_len > 8:
    if "Orders" in head_list:
        print("There's orders")
        #kwp_o.append(get_orders(file))
        tf_order = True
        if "Revenue" in head_list:
            print("There's Orders and Revenue")
            #kwp_r.append(get_revenue(file))
            tf_rev = True
        else:
            print('No Revenue')
            tf_rev = False
    elif "Revenue" in head_list:
        print("There's Orders, but no Revenue")
        #kwp_r.append(get_revenue(file))
        tf_rev = True
    else:
        print('No Orders or revenue')
        tf_order = False
        tf_rev = False
else:
    print('No Orders Or Revenue, Continuing Process...')
    tf_order = False
    tf_rev = False

if 'Orders' in head_list:
    for file in dirs:
        if file.find("BE_SearchEngine") != -1:
            if file.find(".csv") != -1:
                kwp_o.append(get_orders(file))                  #Get Orders
            else:
                continue
        else:
            continue

if 'Revenue' in head_list:
    for file in dirs:
        if file.find("BE_SearchEngine") != -1:
            if file.find(".csv") != -1:
                kwp_r.append(get_revenue(file))                 #Get Orders
            else:
                continue
        else:
            continue
        

for file in dirs:
    if file.find("BE_SearchEngine") != -1:
        if file.find(".csv") != -1:
            kwp_d.append(file[len(file)-12:len(file)-4])    #Get Date
            kwp_v.append(get_sum(file))                     #Get Visits
            kwp_pv.append(get_page_views(file))             #Get Page Views
            kwp_date.append(get_date(file))                 #NEW get date
            kwp_se.append(get_se(file))                     #NEW get search engines
            kwp_ep.append(get_ep(file))                     #NEW get entry pages
            kwp_allPages.append(get_pages(file))
        else:
            continue
    else:
        continue

if tf_order == True or tf_rev == True:
    if list_len > 10:
        x = 10
        while x < list_len:
            # print(head_list[x])
            conv_name.append(head_list[x])
            x = x + 1
    else:
        print("There's no conversions")
elif tf_order == False or tf_rev == False:
    if list_len > 8:
        x = 8
        while x < list_len:
            # print(head_list[x])
            conv_name.append(head_list[x])
            x = x + 1
    else:
        print("There's no conversions")

#print(conv_name) #For QA

file_name = get_file_name + ".xlsx"
writer = pd.ExcelWriter(file_name, engine='xlsxwriter')

data_df4 = {'Page Views': kwp_pv
            ,'Visits': kwp_v
            ,'Date': kwp_d
            #,'Orders': kwp_o
            #,'Revenue': kwp_r
            }
df4 = pd.DataFrame(data_df4,
                   columns=['Date'
                            ,'Visits'
                            ,'Page Views'
                            #,'Orders'
                            #,'Revenue'
                            ])
if save_pages == 'y':
    result = pd.concat(kwp_allPages)
    df5 = pd.DataFrame(result,
                       columns=['Date'
                                ,'Search Engines'
                                ,'Mobile Device Type'
                                ,'Entry Pages'
                                ,'Visits'
                                ,'Page Views'
                                ])
    df5.to_excel(writer, sheet_name='Entry Pages Total')


l_ea = []
temp_conv = []

for each in conv_name:
    print('Combining Conversion: ',each,'\n--------------------')
    for file in dirs:
        if file.find("BE_SearchEngine") != -1:
            if file.find(".csv") != -1:
                temp_conv.append(get_conv(file,each))
            else:
                continue
        else:
            continue
    kwp_conv.append(temp_conv)
    temp_conv = []

data_dfz = {'Date': kwp_d}
dfz = pd.DataFrame(data_dfz, columns=['Date'])

if len(conv_name) > 0:
    if len(kwp_conv) == 5:
        data_dfx = {'Date': kwp_d,
                conv_name[0]:kwp_conv[0],
                conv_name[1]:kwp_conv[1],
                conv_name[2]:kwp_conv[2],
                conv_name[3]:kwp_conv[3],
                conv_name[4]:kwp_conv[4]}
        dfx = pd.DataFrame(data_dfx, columns=[
            'Date',
            conv_name[0],
            conv_name[1],
            conv_name[2],
            conv_name[3],
            conv_name[4]])
    elif len(kwp_conv) == 4:
        data_dfx = {'Date': kwp_d,
                conv_name[0]:kwp_conv[0],
                conv_name[1]:kwp_conv[1],
                conv_name[2]:kwp_conv[2],
                conv_name[3]:kwp_conv[3]}
        dfx = pd.DataFrame(data_dfx, columns=[
            'Date',
            conv_name[0],
            conv_name[1],
            conv_name[2],
            conv_name[3]])
    elif len(kwp_conv) == 3:
        data_dfx = {'Date': kwp_d,
                conv_name[0]:kwp_conv[0],
                conv_name[1]:kwp_conv[1],
                conv_name[2]:kwp_conv[2]}
        dfx = pd.DataFrame(data_dfx, columns=[
            'Date',
            conv_name[0],
            conv_name[1],
            conv_name[2]])
    elif len(kwp_conv) == 2:
        data_dfx = {'Date': kwp_d,
                conv_name[0]:kwp_conv[0],
                conv_name[1]:kwp_conv[1]}
        dfx = pd.DataFrame(data_dfx, columns=[
            'Date',
            conv_name[0],
            conv_name[1]])
    elif len(kwp_conv) == 1:
        data_dfx = {'Date': kwp_d,
                conv_name[0]:kwp_conv[0]}
        dfx = pd.DataFrame(data_dfx, columns=['Date',conv_name[0]])
            
if len(conv_name) > 0:
    dfx.to_excel(writer, sheet_name='Conversions')

if len(kwp_r) > 0:
    if len(kwp_o) > 0:
        data_dfro = {'Date':kwp_d, 'Orders':kwp_o, 'Revenue':kwp_r}
        dfro = pd.DataFrame(data_dfro, columns=['Date','Orders','Revenue'])
        dfro.to_excel(writer, sheet_name='Revenue & Orders')
    else:
        data_dfro = {'Date':kwp_d, 'Revenue':kwp_r}
        # print('Dates: ',len(kwp_d))   #For QA
        # print('Rev: ',len(kwp_r))     #For QA
        dfro = pd.DataFrame(data_dfro, columns=['Date','Revenue'])
        dfro.to_excel(writer, sheet_name='Revenue')
elif len(kwp_o) > 0:
    data_dfro = {'Date':kwp_d, 'Orders':kwp_o}
    dfro = pd.DataFrame(data_dfro, columns=['Date','Orders'])
    dfro.to_excel(writer, sheet_name='Orders')

df4.to_excel(writer, sheet_name='KWP')

writer.save()



I = 0
while I < len(fl_KWPage):
    fle = fl_KWPage[I]
    print(fle, "removed")
    os.remove(fle)
    if file_save == 'N' or file_save == 'n':
        fle2 = fle[:len(fle)-4] + ".csv"
        os.remove(fle2)
        print(fle2, "removed")
        I = I + 1
    else:
        I = I + 1
        
print('\nI have exported the data to:',get_file_name,'Excel File\n')
check = input('Press ENTER to exit')
