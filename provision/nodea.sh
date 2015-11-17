
HADOOP_USERNAME=huser
cp /vagrant_host/hadoop_installation/conf/slaves /home/$HADOOP_USERNAME/hadoop/etc/hadoop/

ssh-keygen -t rsa -P '' -f /home/$HADOOP_USERNAME/
