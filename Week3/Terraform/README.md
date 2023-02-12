## How to run this terraform

### 1. Rename file local-values.json.example to local-values.json
### 2. Change content of local-values-json
```json
  {
  "data_lake_bucket": "<your data lake bucket>",
  "credential_file": "<your credential file>",
  "project_id": "<your project id>"
}
```
### 3. Run
```shell
  terrafom init
``` 
### 4. Run
```shell
  terraform apply
```
### 5. Terraform asks to perform action, type 'yes'
```shell
Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve

  Enter a value: yes
```