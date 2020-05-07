#! /bin/sh

# set -o errexit
# set -o pipefail
# set -o nounset 

###########################################################
host_ip=1
user=
password=
path=path
local_dir=/tmp/wdc
# local_dir=/home/h/Documents/excel/bo/ftp/files
local_record=local_record.log
filename=fname
table=table
last_day=`date -d "last day" +%Y%m%d`
current_day=`date +%Y%m%d`
local_path=$local_dir/$path
num=10
###########################################################


function usage(){
	echo "####################USAGE####################################"
	echo "params: ip user password path filename table"
	echo "#ftpip"
	echo "#ftpusername"
	echo "#ftppassword"
	echo "#ftppath"
	echo "#ftpfilenameRegrex"
	echo "#hivetablename"
	echo "#num"
	echo "###################params is not right exist #################"
}




function setupparams(){
	host_ip=$1
	user=$2
	password=$3
	path=$4
	filename=$5
	table=$6
	num=$7
	local_path=$local_dir/$path
	# 创建目录 
	# rm  $local_path/*
	mkdir $local_path -p

	echo "-----------------输入参数begin-----------------"
	echo "ip:$host_ip"
	echo "user:$user"
	echo "pasword:$password"
	echo "path:$path"
	echo "filename:$filename"
	echo "table:$table"
	echo "num:$num"
	echo "local_path:$local_path"
	echo "-----------------输入参数end-----------------"
}



function load_records(){
	cd $local_path
	filename=$1
	ftp -n $host_ip <<endl
	user $user  $password
	prompt
	cd $path
	lcd $local_path
	ls $filename
	bye
endl
}



#取回该目录下所有文件名。
function checkfile(){
	cd $local_path
	filename=$1
	#$(load_records $filename >$local_record)	
	res=0
	if [ `grep -c ${filename}.OK ${local_record}` -eq 1 ];then
		if [ `grep -c $filename.ZIP ${local_record}` -eq 1 ];then
			echo true
		else
			echo false
		fi
	else
	 #echo "文件不存在"
	 echo false
	fi
}


function loadfile(){
	echo "####################step3########################"
	echo "##################loading files->$(pwd)########################"
	echo "wget ftp://$user:$password@$host_ip/$path/$1.ZIP"
	$(wget ftp://$user:$password@$host_ip/$path/$1.ZIP)
echo 1
}






function unzipfile(){
	echo "####################step4########################"
	echo "##################unzip file $1########################"
	filename=$1.ZIP
	cd $local_path
	echo "unzip file $filename"
	res=$(unzip $filename)
	# 解压出的文件重命名
	# mv *.txt  $1.txt
	mv *.txt $1
	gzip $1
}



# tmptable->tmptable append to  inctable
function loaddata2hive(){
	echo "####################step5########################"
	echo "##################loading file to hive $1########################"
	filename=$1.gz
	# echo "$local_dir/$filename"
	cd $local_path
	p=`pwd`
	# echo $p/$filename
	# echo $local_dir/$filename

	if [[ -f $local_path/$filename ]];then
	# if [[ -f /tmp/wdc/RMS_STOCK_MOVING_20200406.txt ]];then
		echo "${filename}获取成功!准备导入..."
		# echo `date "+%Y-%m-%d %H:%M:%S"` ":去掉第一行"
		# sed -i '1d' ${path}/${filename}
		echo `date "+%Y-%m-%d %H:%M:%S"` ":将文件上传至HDFS:/hive/data/"${filename}
		hdfs dfs -put -f $local_path/${filename} /hive/data/
		echo `date "+%Y-%m-%d %H:%M:%S"` ":上传完成!"

		echo `date "+%Y-%m-%d %H:%M:%S"` ":将文件${filename}从HDFS导入${table}中..."
		# 分区。split('_')[-1]
		p_f=`echo $1 | sed 's/.*_\([^:]*\)$/\1/'`
		# 建表/临时表，指定txtfile格式，导数据入。再select override partition (da='temp')"
		if [[ $table =~ _full$ ]]; then 
			hql="load data inpath '/hive/data/"${filename}"' overwrite into table ${table} partition(p_f='full') "
		else 
			hql="load data inpath '/hive/data/"${filename}"' overwrite into table ${table} partition(p_f=$p_f) "
		fi
		
		echo "$hql"
		hive -e "${hql}"

	else
		echo "load data2 hive 失败!，文件名：$filename"
	fi

}



function buildrealfilename(){
	last=$1
	# currentday
	# today=`date +%Y%m%d`
	day=`date -d "$last day ago" +%Y%m%d`
	currentday=$day
	# realname=${filename/\*/$today}.ZIP
	realname=${filename/\*/$day}
	echo $realname
}




function cleanlocaldata(){
	echo "####################step6########################"
	echo "##################clean local files########################"
	cd $local_path
	rm $1.*
}

# 删除本地文件，ftp文件
function bakftpdata(){
	echo "####################step7########################"
	echo "##################clean ftp files########################"
	filename=$1
	# filename=周报4月8日-4月9日.xlsx
	ftp -n $host_ip <<endl
	user $user  $password
	prompt
	mkdir bak/$path
	rename $path/$filename.ZIP bak/$path/$filename.ZIP
	mdelete $path/$filename.OK
	mdelete $path/$filename.LOCK
	bye
endl
}






if [ $# -lt 7 ];
	then
	    usage
	    exit 1
fi
echo "输入参数: $@"
# 初始化参数
setupparams $@




function main(){
	echo "####################MAIN########################"
	# 下载所有文件列表

	echo "####################step1########################"
	echo "###########loading records...###########"
	load_records $filename>$local_path/$local_record

	for i in `seq 0 $num`
        do
        
			# 检查ok文件。目标文件。 time.最近3天。
			filenameprefix=$(buildrealfilename $i)
			echo "================>>>>>$filenameprefix"


			# 检查okfile与zipfile.同时存在

			echo "####################step2########################"
			echo "##################checkfile######################"
			res=$(checkfile $filenameprefix)

			if [[ $res == true ]];then
				# 文件符合规则，下载文件
				msg=$(loadfile $filenameprefix)
				# 解压文件.解压出的文本文件名字不一致。
				msg=$(unzipfile $filenameprefix)
				# 上传到hdfs/和hive中。
				msg=$(loaddata2hive $filenameprefix)
				msg=$(cleanlocaldata $filenameprefix)
				# 删除okfile/data/lock文件
				msg=$(bakftpdata $filenameprefix)
				echo "DONE!"
			else
				echo 'file not exist or network '
			fi

        done
	echo "####################ALL DONE!########################"

}

main