#!/bin/sh
#set -vx

AUTHOR="kihyuk (kwon.kihyuk@oracle.com)"
VERSION="9.1.0"
MYSQL="MySQL_"${VERSION}
MSHELL="MySQL_Shell_"${VERSION}

ERR=0

# Setup general language type
export LANG=en_US.UTF-8

# Used for a better dialog visualization on putty
export NCURSES_NO_UTF8_ACS=1

# Define the dialog exit status codes
: ${DIALOG_OK=0}
: ${DIALOG_CANCEL=1}
: ${DIALOG_HELP=2}
: ${DIALOG_EXTRA=3}
: ${DIALOG_ITEM_HELP=4}
: ${DIALOG_ESC=255}

export working_dir="$( dirname $0 )"
if [[ ! ${working_dir} =~ ^/ ]]; then
    working_dir="$( pwd)/${working_dir}"
fi

export log_file="${working_dir}/$(basename -s .sh $0).log"
export sw_dir="${working_dir}/pkg"
export pass_file="${working_dir}/.db_temp_pass.lst"
export oci_secret_file="${working_dir}/.passwd-s3fs"

export DEV_LINK_SHELL_TAR='https://dev.mysql.com/get/Downloads/MySQL-Shell/mysql-shell-9.1.0-1.el8.x86_64.rpm'

export AIRPORT_DB='https://downloads.mysql.com/docs/airport-db.tar.gz'

#####################################################
# FUNCTIONS
#####################################################

# Display message
display_msg() {
    dialog --title "$1" \
	--backtitle "Message Display" \
        --no-collapse \
	--msgbox "$2" 0 0
}

# Exit from errors
stop_execution_for_error () {
# first parameter is the exiut code
# second parameter is the error message

    ERR=$1
    ERR=${ERR:=1}

    MSG=$2
    MSG=${MSG:="Generic error"}
    echo "$(date) - ERROR - ${MSG}" |tee -a ${log_file}
    echo "$(date) - INFO - End" >> ${log_file}

    exit $ERR
}

install_mysql_utilites () {
    ERR=0

    echo "$(date) - INFO - Start function ${FUNCNAME[0]}" >> ${log_file}

    # Install s3fs for mounting object storage 
    echo "$(date) - INFO - Install s3fs client and Shell on $client" |tee -a ${log_file}    
    wget --progress=dot $DEV_LINK_SHELL_TAR -O "${sw_dir}/`basename $DEV_LINK_SHELL_TAR`" 2>&1 | stdbuf -o0 awk '/[.] +[0-9][0-9]?[0-9]?%/ { print substr($0,63,3) }' | dialog --backtitle "MySQL configuration" --gauge "Download MySQL shell (community ${VERSION})" 10 60
    SHL_RPM_DOWNLOAD_STATUS=$?
    if [ $SHL_RPM_DOWNLOAD_STATUS -ne 0 ] ; then
       msg="ERROR - Error during the download of MySQL shell"
       echo "$(date) - ${msg}" |tee -a ${log_file}
       display_msg "Download Error" $msg
    fi
   
    clear;

    if [ -f "${sw_dir}/`basename $DEV_LINK_SHELL_TAR`" ] ; then
       sudo yum -y install ${sw_dir}/*shell-${VERSION}-*x86_64.rpm
    else 
       ERR=1
       msg="ERROR - Install Doesn't exist mysql-shell install file"
       echo "$(date) - ${msg}" |tee -a ${log_file}
       display_msg "Install Error" "${msg}"
       return $ERR
    fi

    ERR=$?

    if [ $ERR -eq 0 ]
    then
       msg="MySQL shell installation completed"
       display_msg "Client installation" "${msg}"
       echo "$(date) - INFO - ${msg}" >> ${log_file}
    else
       msg="MySQL shell installation failed"
       display_msg "Client installation" "${msg}"
       echo "$(date) - INFO - ${msg}(${ERR})" >> ${log_file}
    fi

    echo "$(date) - INFO - End function ${FUNCNAME[0]}" >> ${log_file}

    return $ERR
}

get_mysql_info () {

    ERR=0

    echo "$(date) - INFO - Start function ${FUNCNAME[0]}" >> ${log_file}

    DB_IP=`cat $pass_file | cut -d " " -f 1 -s`
    DB_PORT=`cat $pass_file | cut -d " " -f 2 -s`
    DB_USER=`cat $pass_file | cut -d " " -f 3 -s`
    DB_PASS=`cat $pass_file | cut -d " " -f 4 -s`

    exec 3>&1

    result=$(dialog \
        --title "Database info" \
        --backtitle "Connection test" \
        --clear \
        --no-collapse \
        --cancel-label "Exit" \
        --form "Put MySQL Server Info" 10 60 0 \
        "ip  (↑): " 1 1 "${DB_IP}"   1 12 25 0 \
        "port   : " 2 1 "${DB_PORT}" 2 12 10 0 \
        "user   : " 3 1 "${DB_USER}" 3 12 25 0 \
        "pass(↓): " 4 1 "${DB_PASS}" 4 12 25 0 \
        2>&1 1>&3)

    exit_status=$?

    exec 3>&-

    case $exit_status in
    $DIALOG_CANCEL)
        clear
        echo "$(date) - INFO - Scripts menu end" >> ${log_file}
        return ""
        ;;
    $DIALOG_ESC)
        clear
        echo "$(date) - INFO - Scripts menu cancelled" >> ${log_file}
        return ""
        ;;
    esac

    echo $result > ${pass_file}

    echo "$(date) - INFO - End function ${FUNCNAME[0]}" >> ${log_file}

    return $result
}

connect_mysql_server () {
 
    ERR=0

    echo "$(date) - INFO - Start function ${FUNCNAME[0]}" >> ${log_file}
    
    get_mysql_info
    $result=$?

    if [ "$result" = "" ]; then
       return $ERR 
    fi

    DB_IP=`echo $result | cut -d " " -f 1 -s`
    DB_PORT=`echo $result | cut -d " " -f 2 -s`
    DB_USER=`echo $result | cut -d " " -f 3 -s`
    DB_PASS=`echo $result | cut -d " " -f 4 -s`
    msg=`echo "ip:${DB_IP}\nport:${DB_PORT}\nuser:${DB_USER}\npass:${DB_PASS}\n"`
   
    if [ -z "${DB_IP}" ] || [ -z "${DB_PORT}" ] || [ -z "${DB_USER}" ] || [ -z "${DB_PASS}" ] 
    then
       display_msg "Error - Wrong info" "${msg}"
       echo "$(date) - ERROR - ${msg}" >> ${log_file}
       return $ERR
    fi

    echo "$(date) - INFO - DB INFO - ${msg}" >> ${log_file}

    echo "$(date) - INFO - test connectivity for MySQL" >> ${log_file}
    result=$(sudo /mysql/mysql-latest/bin/mysql -u${DB_USER} -h${DB_IP} -P${DB_PORT} -p${DB_PASS} -e "show databases")
    ERR=$?
    if [ $ERR -ne 0 ]
    then
       msg="The database can't be connect using your db info"
       display_msg "Error - Connection" "${msg}\n${result}"
       echo "$(date) - ERROR - ${msg}" >> ${log_file}
    else 
       display_msg "Connection success" "${result}"
       echo "$(date) - INFO - Connection test is ok" >> ${log_file}
    fi

    echo "$(date) - INFO - End function ${FUNCNAME[0]}" >> ${log_file}
}

load_data () {
    
    ERR=0

    echo "$(date) - INFO - Start function ${FUNCNAME[0]}" >> ${log_file}

    sudo rm -f ${sw_dir}/airportdb.tar.gz
    echo "$(date) - INFO - Download of airport db... please wait..." >> ${log_file}
    wget --progress=dot --secure-protocol=auto -O "${sw_dir}/airportdb.tar.gz" ${AIRPORT_DB} 2>&1 | stdbuf -o0 awk '/[.] +[0-9][0-9]?[0-9]?%/ { print substr($0,63,3) }' | dialog --backtitle "Data Download" --gauge "Download airport data from repo" 10 60

    cd ${sw_dir}
    sudo rm -rf ${sw_dir}/airport-db
    sudo tar xf ${sw_dir}/airportdb.tar.gz 
    DB_DOWNLOAD_STATUS=$?

    if [ $DB_DOWNLOAD_STATUS -ne 0 ] ; then
       msg="ERROR - Error during the download of airport db"
       echo "$(date) - ${msg}" |tee -a ${log_file}
       display_msg "Download Error" "${msg}"

       return $DB_DOWNLOAD_STATUS
    fi

    get_mysql_info
    $result=$?

    if [ "$result" = "" ]; then
       return $ERR
    fi    

    DB_IP=`echo $result | cut -d " " -f 1 -s`
    DB_PORT=`echo $result | cut -d " " -f 2 -s`
    DB_USER=`echo $result | cut -d " " -f 3 -s`
    DB_PASS=`echo $result | cut -d " " -f 4 -s`
    msg=`echo "ip:${DB_IP}\nport:${DB_PORT}\nuser:${DB_USER}\npass:${DB_PASS}\n"`

    if [ -z "${DB_IP}" ] || [ -z "${DB_PORT}" ] || [ -z "${DB_USER}" ] || [ -z "${DB_PASS}" ]
    then
       display_msg "Error - Wrong info" "${msg}"
       echo "$(date) - ERROR - ${msg}" >> ${log_file}
       return $ERR
    fi
    
    clear
    sudo mysqlsh -u${DB_USER} -p"${DB_PASS}" -h ${DB_IP} -- util loadDump ${sw_dir}/airport-db --ignoreVersion --resetProgress --threads 10 2>&1
    ERR=$?
    if [ $ERR -ne 0 ] ; then
       msg="ERROR - Error during loading airport db"
       echo "$(date) - ${msg}" |tee -a ${log_file}
       display_msg "Loading Data Error" "${msg}"

       return $ERR
    fi   

    display_msg "Download Completed" "The airport db load is completed"

    echo "$(date) - INFO - End function ${FUNCNAME[0]}" >> ${log_file}
}

setup_acess_key () {

    ERR=0

    echo "$(date) - INFO - Start function ${FUNCNAME[0]}" >> ${log_file}

    exec 3>&1

    result=$(dialog \
        --title "setup authorization of oci" \
        --backtitle "access and secret key" \
        --clear \
        --no-collapse \
        --cancel-label "Exit" \
        --form "Put access and secret key" 10 60 0 \
        "Access Key (↑): " 1 1 ""   1 16 60 0 \
        "Secret Key (↓): " 2 1 ""   2 16 60 0 \
        2>&1 1>&3)

    exit_status=$?

    exec 3>&-

    case $exit_status in
    $DIALOG_CANCEL)
        clear
        echo "$(date) - INFO - Scripts menu end" >> ${log_file}
        return $ERR
        ;;
    $DIALOG_ESC)
        clear
        echo "$(date) - INFO - Scripts menu cancelled" >> ${log_file}
        return $ERR
        ;;
    esac

    echo $result >> ${log_file}    

    ACCESS_KEY_ID=`echo $result | cut -d " " -f 1 -s`
    SECRET_ACCESS_KEY=`echo $result | cut -d " " -f 2 -s`

    echo "${ACCESS_KEY_ID}:${SECRET_ACCESS_KEY}" > $oci_secret_file

    ERR=$?

    if [ $ERR -eq 0 ]
    then
       msg="Making access key was completed"
       display_msg "Setup access and secret key" "${msg}"
       echo "$(date) - INFO - ${msg}" >> ${log_file}
    else
       msg="Making access key was failed"
       display_msg "Setup access and secret key" "${msg}"
       echo "$(date) - INFO - ${msg}(${ERR})" >> ${log_file}
    fi    

    sudo chmod 400 ${oci_secret_file}

    echo "$(date) - INFO - End function ${FUNCNAME[0]}" >> ${log_file}
}

mount_object_storage () {

    ERR=0

    echo "$(date) - INFO - Start function ${FUNCNAME[0]}" >> ${log_file}

    title="Mount Object Storage"

    exec 3>&1

    result=$(dialog \
	--title "$title" \
        --clear \
        --no-collapse \
        --cancel-label "Exit" \
	--backtitle "Choose region" \
	--radiolist "Choose cloud region" 15 50 2 \
	"ap-seoul-1" "seoul region" ON \
	"ap-chuncheon-1" "chuncheon region" off \
        2>&1 1>&3)

    exit_status=$?

    exec 3>&-


    echo "selected region: "$result >> ${log_file}

    case $exit_status in
    $DIALOG_CANCEL)
        clear
        echo "$(date) - INFO - Scripts menu end" >> ${log_file}                           
        return $ERR
        ;;
    $DIALOG_ESC)
        clear
        echo "$(date) - INFO - Scripts menu cancelled" >> ${log_file}                     
        return $ERR
        ;;
    esac
    
    exec 3>&1

    region=$result

    result=$(dialog \
        --title "$title" \
        --backtitle "Input rest info" \
        --clear \
        --no-collapse \
        --cancel-label "Exit" \
        --form "Setup rest info" 10 60 0 \
        "region   (↑): " 1 1 "${region}"   1 15 25 0 \
        "namespace   : " 2 1 " " 2 15 25 0 \
        "bucket      : " 3 1 " " 3 15 25 0 \
        "mount loc(↓): " 4 1 " " 4 15 25 0 \
        2>&1 1>&3)    

    exit_status=$?

    exec 3>&-    

    echo "inputed value: "$result >> ${log_file}

    case $exit_status in
    $DIALOG_CANCEL)
        clear
        echo "$(date) - INFO - Scripts menu end" >> ${log_file}
        return $ERR
        ;;
    $DIALOG_ESC)
        clear
        echo "$(date) - INFO - Scripts menu cancelled" >> ${log_file}
        return $ERR
        ;;
    esac

    MP_REGION=`echo $result | cut -d " " -f 1 -s`
    MP_NAMESP=`echo $result | cut -d " " -f 2 -s`
    MP_BUCKET=`echo $result | cut -d " " -f 3 -s`
    MP_LOCA=`echo $result | cut -d " " -f 4 -s`
    msg=`echo "region:${MP_REGION}\nnamespace:${MP_NAMESP}\nbucket:${MP_BUCKET}\nlocation:${MP_LOCA}\n"`

    if [ -z "${MP_REGION}" ] || [ -z "${MP_NAMESP}" ] || [ -z "${MP_BUCKET}" ] || [ -z "${MP_LOCA}" ]
    then
       display_msg "Error - Wrong info" "${msg}"                                          
       echo "$(date) - ERROR - ${msg}" >> ${log_file}                                     
       return $ERR
    fi

    sudo s3fs ${MP_BUCKET} ${MP_LOCA} -o endpoint=${MP_REGION} -o passwd_file=${oci_secret_file} -o url=https://${MP_NAMESP}.compat.objectstorage.${MP_REGION}.oraclecloud.com/ -onomultipart -o use_path_request_style 2>> ${log_file}

    ERR=$?

    if [ $ERR -ne 0 ] ; then
       msg="ERROR - Can't mount object storage"
       echo "$(date) - ${msg}" |tee -a ${log_file}
       display_msg "Mount Error" "${msg}"
       return $ERR
    else 
       msg="Success - Mount of Object storage was completed"
       display_msg "Mount Succeed" "${msg}"
       echo "$(date) - INFO - ${msg}" >> ${log_file}
    fi

    echo "$(date) - INFO - End function ${FUNCNAME[0]}" >> ${log_file}
}

###################################################################################################
# MAIN
###################################################################################################

echo "$(date) - INFO - Start" >> ${log_file}
echo "$(date) - INFO - Script version ${VERSION}" >> ${log_file}

os_version=`cat /etc/os-release | grep "VERSION=" | awk -F\" '{print $2}'`

if [[ ! "${os_version}" =~ "8." ]] && [[ ! "${os_version}" =~ "9." ]] ; then
    echo "Os verion must use greater than 7"
    exit 
fi

echo "$(date) - INFO - Check, and if needed install, install pre-requisites" | tee -a ${log_file}

sudo yum -y -q install ncurses-compat-libs dialog wget unzip jq python39-libs 2>&1 >>${log_file}
sudo mkdir -p ${sw_dir} 2>&1 >>${log_file}
sudo chown -R $USER ${sw_dir} 2>&1 >>${log_file}

if [[ "${os_version}" =~ "8." ]]; then
    sudo yum-config-manager --enable ol8_baseos_latest ol8_appstream ol8_addons ol8_developer_EPEL
else
    sudo yum-config-manager --enable ol9_baseos_latest ol9_appstream ol9_addons ol9_developer_EPEL
fi

sudo yum install s3fs-fuse -y

sudo chmod +x /usr/bin/fusermount 2>&1 >>${log_file}


ERR=$?
if [ $ERR -ne 0 ] ; then
    stop_execution_for_error $ERR "Issues during required software installation"
fi

if [ $OPTIND -eq 1 ]; then
    echo "$(date) - INFO - Interactive mode" >> ${log_file}

    while true
    do
    	exec 3>&1

	selection=$(dialog --keep-tite \
		--backtitle "Heatwavae ${VERSION} Lakehouse setup" \
		--title "Lakehouse setup menu" \
		--clear  \
		--cancel-label "Exit" \
		--menu "\nEnter follow number to use these commands" 0 0 0\
		"1" "Install mysql shell" \
		"2" "Setup Acess and Secret key" \
		"3" "Mount Object storage in filesystem" \
		"4" "Test connectivity of MySQL" \
		"5" "Load Airport data" \
		2>&1 1>&3)

        exit_status=$?

        # Close file descriptor 3
        exec 3>&-

	case $exit_status in
        $DIALOG_CANCEL)
            clear
            echo "$(date) - INFO - Interactive menu end" >> ${log_file}
            exit
            ;;
        $DIALOG_ESC)
            clear
            echo "$(date) - INFO - Interactive menu cancelled" >> ${log_file}
            return $ERR
            ;;
        esac

	case $selection in
	1 )
	    clear
	    install_mysql_utilites
	    ;;
	2 )
	    clear
	    setup_acess_key
	    ;;
        3 )
            clear
	    mount_object_storage
	    ;;
	4) 
	    connect_mysql_server
	    ;;
	5)
            load_data
            ;;
	esac

    done
fi

echo "$(date) - INFO - End" >> ${log_file}
exit $ERR
