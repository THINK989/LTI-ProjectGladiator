 #!/bin/bash
sqoop import \
      --connect jdbc:mysql://localhost/WorldBankLoan_DB \
      --username hiveuser \
      --password hiveroot \
      --table loanStatement \
      --hive-database WorldBankLoan_DB \
      --hive-table loanstatement \
      --target-dir hdfs://localhost:9000/user/root/hiveEtxTable/WorldBankLoan \
      --create-hive-table \
      --fields-terminated-by '\t'\
      -m 1


sqoop import \
      --connect jdbc:mysql://localhost/WorldBankLoan_DB \
      --username hiveuser \
      --password hiveroot \
      --table loanStatement \		
      --append \
      --hive-database WorldBankLoan_DB \
      --hive-table loanstatement \		
      --check-column end_of_period \
      --last-value date \
      --incremental lastmodified \
      --fields-terminated-by '\t' \
      --target-dir hdfs://localhost:9000/user/root/hiveEtxTable/WorldBankLoan \
      -m 1
