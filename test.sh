#!/usr/bin/bash
gcc lrr.c -o lrr
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'
PATH=$PATH:.
HELP_MESSAGE="Usage: lrr [SUBCOMMAND] [ARGUMENTS]"
PUNISH_SUBCOMMAND_HELP="Usage: lrr punish [ARGUMENTS]"
QUERY_SUBCOMMAND_HELP="Usage: lrr query [ARGUMENTS]"
DAEMON_SUBCOMMAND_HELP="Usage: lrr daemon"
HELP_HELP_MESSAGE="Usage: lrr help
       lrr help [SUBCOMMAND] [ARGUMENTS]

Prints help information (generally or for a specific subcommand)."
if [ "$(lrr help help)" == "$HELP_HELP_MESSAGE" ]; then
  echo -e "[$GREEN OK $NC] help subcommand prints help message for help subcommand"
else
  echo -e "[$RED FAIL $NC] help subcommand does not print proper help message for help subcommand"
fi
lrr help help 2>/dev/null >/dev/null;
if [ $? -eq 0 ]; then
  echo -e "[$GREEN OK $NC] help subcommand exits with 0 when given a valid subcommand"
else
  echo -e "[$RED FAIL $NC] help subcommand does not exit with 0 when given a valid subcommand"
fi
lrr help badarg 2>/dev/null >/dev/null;
if [ $? -eq 1 ]; then
  echo -e "[$GREEN OK $NC] help subcommand exits with 1 when given an invalid subcommand"
else
  echo -e "[$RED FAIL $NC] help subcommand does not exit with 1 when given an invalid subcommand"
fi
if [ "$(lrr)" == "$HELP_MESSAGE" ]; then
  echo -e "[$GREEN OK $NC] Bare command usage prints help message"
else
  echo -e "[$RED FAIL $NC] Improper help message on bare command usage"
fi
if [ "$(lrr help)" == "$HELP_MESSAGE" ]; then
  echo -e "[$GREEN OK $NC] help subcommand prints help message"
else
  echo -e "[$RED FAIL $NC] help subcommand does not print proper help message"
fi
if [ "$(lrr help query)" == "$QUERY_SUBCOMMAND_HELP" ]; then
  echo -e "[$GREEN OK $NC] help subcommand prints help message for query subcommand (on extra arg: query)"
else
  echo -e "[$RED FAIL $NC] help subcommand does not print proper help message for query subcommand (on extra arg: query)"
fi
lrr help query 2>/dev/null >/dev/null
if [ $? -eq 0 ]; then
  echo -e "[$GREEN OK $NC] help subcommand exits with 0 when given a valid subcommand (query)"
else
  echo -e "[$RED FAIL $NC] help subcommand does not exit with 0 when given a valid subcommand (query)"
fi
lrr help punish 2>/dev/null >/dev/null
if [ $? -eq 0 ]; then
  echo -e "[$GREEN OK $NC] help subcommand exits with 0 when given a valid subcommand (punish)"
else
  echo -e "[$RED FAIL $NC] help subcommand does not exit with 0 when given a valid subcommand (punish)"
fi
if [ "$(lrr help punish)" == "$PUNISH_SUBCOMMAND_HELP" ]; then
  echo -e "[$GREEN OK $NC] help subcommand prints help message for punish subcommand (on extra arg: punish)"
else
  echo -e "[$RED FAIL $NC] help subcommand does not print proper help message for punish subcommand (on extra arg: punish)"
fi
if [ "$(lrr punish)" == "$PUNISH_SUBCOMMAND_HELP" ]; then
  echo -e "[$GREEN OK $NC] punish subcommand prints help message"
else
  echo -e "[$RED FAIL $NC] punish subcommand does not print proper help message"
fi
if [ "$(lrr punish 1 <<<1)" == "$PUNISH_SUBCOMMAND_HELP" ]; then
  echo -e "[$GREEN OK $NC] punish subcommand prints help message when conflicting input methods given"
else
  echo -e "[$RED FAIL $NC] punish subcommand does not print proper help message when conflicting input methods given"
fi
if [ "$(lrr query)" == "$QUERY_SUBCOMMAND_HELP" ]; then
  echo -e "[$GREEN OK $NC] query subcommand prints help message"
else
  echo -e "[$RED FAIL $NC] query subcommand does not print proper help message"
fi
if [ "$(lrr daemon 1)" == "$DAEMON_SUBCOMMAND_HELP" ]; then
	echo -e "[$GREEN OK $NC] daemon subcommand prints help message when an argument is given"
else
	echo -e "[$RED FAIL $NC] daemon subcommand does not print proper help message when an argument is given"
fi
if [ "$(lrr daemon 1 <<<1)" == "$DAEMON_SUBCOMMAND_HELP" ]; then
	echo -e "[$GREEN OK $NC] daemon subcommand prints help message when both input methods used"
else
	echo -e "[$RED FAIL $NC] daemon subcommand does not print proper help message when both input methods used"
fi
if [ "$(lrr daemon <<<1)" == "$DAEMON_SUBCOMMAND_HELP" ]; then
	echo -e "[$GREEN OK $NC] daemon subcommand prints help message when input is given through stdin"
else
	echo -e "[$RED FAIL $NC] daemon subcommand does not print proper help message when input is given through stdin"
fi
if [ "$(lrr help <<<1 2>/dev/null)" ]; then
	echo -e "[$GREEN OK $NC] help subcommand parses input from stdin and gives advice"
else
	echo -e "[$RED FAIL $NC] help subcommand does not parse input from stdin and give advice"
fi
