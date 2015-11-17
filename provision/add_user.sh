#!/usr/bin/expect
set groupname [lindex $argv 0]
set username [lindex $argv 1]

spawn sudo adduser --ingroup $groupname $username 

expect "password"
send "$username\r"
expect "password"
send "$username\r"
expect "[]"
send "\r"
expect "[]"
send "\r"
expect "[]"
send "\r"
expect "[]"
send "\r"
expect "[]"
send "\r"
expect "Y"
send "y\r"

close
