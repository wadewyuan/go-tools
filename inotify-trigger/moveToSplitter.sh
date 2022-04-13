#!/bin/bash
GW_ID=$(echo "$1" | grep -o -P '(?<=cdr_a2pgw).*(?=_20)')
BASE_DIR=/app01/bin/sms/SMSCDRSplitter/
echo $GW_ID

case $GW_ID in
    03a )
        SUB_DIR=IP23_A2PGW03
    ;;
    03b )
        SUB_DIR=IP24_A2PGW03
    ;;
    03c )
        SUB_DIR=IP25_A2PGW03
    ;;
    03d )
        SUB_DIR=IP26_A2PGW03
    ;;
    04a )
        SUB_DIR=IP27_A2PGW04
    ;;
    04b )
        SUB_DIR=IP28_A2PGW04
    ;;
    04c )
        SUB_DIR=IP29_A2PGW04
    ;;
    04d )
        SUB_DIR=IP30_A2PGW04
    ;;
esac
if [ -z $SUB_DIR ]
then
    echo "GW_ID not found: $GW_ID"
else
    mv $1 $BASE_DIR$SUB_DIR/cdr/
    cd $BASE_DIR$SUB_DIR/
    sh cdrsplitter.sh > $BASE_DIR/nohup.out
fi
