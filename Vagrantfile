# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure(2) do |config|

    config.vm.synced_folder ".", "/vagrant_host"
    config.vm.box_check_update = false
    config.vm.box = "ubuntu/trusty64"
    config.vm.provision "shell", path: "provision/setup.sh" 

    config.vm.provider "virtualbox" do |vb|
        vb.memory = 2048
        vb.cpus = 2
    end

    config.vm.define "NodeA" do |nodea| 
        nodea.vm.network "private_network", ip: "10.10.10.5"
        nodea.vm.hostname = "master-node"
        nodea.vm.network "forwarded_port", guest: 50070, host: 50070
        nodea.vm.network "forwarded_port", guest: 8088, host: 8088
#        nodea.vm.network "forwarded_port", guest: 8042, host: 8042
        nodea.vm.provision "shell", path: "provision/nodea.sh"
    end

    config.vm.define "NodeB" do |nodeb|
        nodeb.vm.network "private_network", ip: "10.10.10.6"
        nodeb.vm.hostname  = "slave-node-1"
#        nodea.vm.network "forwarded_port", guest: 8042, host: 8142
    end

    config.vm.define "NodeC" do |nodec|
        nodec.vm.network "private_network", ip: "10.10.10.7"
        nodec.vm.hostname  = "slave-node-2"
#        nodea.vm.network "forwarded_port", guest: 8042, host: 8242
    end
end
