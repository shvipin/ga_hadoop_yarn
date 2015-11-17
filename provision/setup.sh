sudo apt-get update
sudo apt-get install -y openjdk-7-jdk
sudo apt-get install -y git
sudo apt-get install -y tree
sudo apt-get install -y maven
sudo apt-get install -y expect

HADOOP_GROUPNAME=hadoop
HADOOP_USERNAME=huser
sudo addgroup $HADOOP_GROUPNAME
expect /vagrant_host/provision/add_user.sh $HADOOP_GROUPNAME $HADOOP_USERNAME
sudo adduser $HADOOP_USERNAME sudo

#copy files
cp -r /vagrant_host/hadoop_installation/hadoop-2.7.1 /home/$HADOOP_USERNAME/hadoop

#make huser as owner
chown $HADOOP_USERNAME -R ../$HADOOP_USERNAME/hadoop
chgrp $HADOOP_GROUPNAME -R ../$HADOOP_USERNAME/hadoop

#append hadoop bash variables.
cat /vagrant_host/provision/bashrc >> /home/$HADOOP_USERNAME/.bashrc

cp /vagrant_host/hadoop_installation/conf/core-site.xml /home/$HADOOP_USERNAME/hadoop/etc/hadoop/
cp /vagrant_host/hadoop_installation/conf/hdfs-site.xml /home/$HADOOP_USERNAME/hadoop/etc/hadoop/
cp /vagrant_host/hadoop_installation/conf/yarn-site.xml /home/$HADOOP_USERNAME/hadoop/etc/hadoop/

