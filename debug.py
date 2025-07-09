from datacontract.data_contract import DataContract
from datacontract.export.odcs_v3_exporter import to_odcs_v3
import oyaml as yaml

dc_path = "C:\\Users\\mpernias\\OneDrive - McKesson Corporation\\Desktop\\datacontract-cli\\dl.mba.internal.db2.PMGENDTA.[PRD].yaml"
datacontract = DataContract(
        data_contract_file=dc_path
    )

spec = datacontract.get_data_contract_specification()
odcs_imported = to_odcs_v3(spec)

with open("C:\\Users\\mpernias\\OneDrive - McKesson Corporation\\Desktop\\datacontract-cli\\dl.mba.internal.db2.PMGENDTA2.yaml", 'w', encoding="utf") as file:
  file.write(odcs_imported.to_yaml())
