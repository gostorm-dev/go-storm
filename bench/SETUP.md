# Benchmark Lab Setup (AWS ap-south-1)

Guide to reproduce the two-machine benchmark lab. Replace `<PLACEHOLDER>`
values with your own. Never commit real IPs, subnet or security-group IDs.

## Infrastructure

| Item | Value |
|---|---|
| Instance type | c6i.large (2 vCPU, Intel Cascade Lake) × 2 |
| OS | Ubuntu 24.04 LTS amd64 |
| Region/AZ | ap-south-1, single AZ for both instances |
| Network | Default VPC; traffic between instances uses **private IPs** |
| Cost | ~$0.15/hour total while both run. STOP when done. |

## Steps

### 1. Security group
```bash
VPC_ID=<YOUR_VPC_ID>
MY_IP=<YOUR_PUBLIC_IP>/32

aws ec2 create-security-group --group-name storm-bench-sg \
  --description "go-storm benchmark lab" --vpc-id $VPC_ID   # → <SG_ID>

# SSH only from your current IP
aws ec2 authorize-security-group-ingress --group-id <SG_ID> \
  --protocol tcp --port 22 --cidr $MY_IP

# All traffic between the two lab machines only
aws ec2 authorize-security-group-ingress --group-id <SG_ID> \
  --protocol all --source-group <SG_ID>
```
NOTE: home ISP IPs rotate. If SSH dies mid-session, re-authorize the new IP.

### 2. Launch both instances
```bash
AMI_ID=<UBUNTU_2404_AMD64_AMI>        # find via: aws ec2 describe-images ...
KEY_NAME=<YOUR_KEYPAIR>

for name in storm-bench-server storm-bench-generator; do
  aws ec2 run-instances --image-id $AMI_ID --instance-type c6i.large \
    --key-name $KEY_NAME --security-group-ids <SG_ID> \
    --subnet-id <YOUR_SUBNET_SAME_AZ> \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$name}]"
done
```

### 3. Prepare BOTH machines
```bash
sudo apt-get update && sudo apt-get install -y sysstat golang-go
sudo sysctl -w net.ipv4.ip_local_port_range="1024 65535"
echo "* soft nofile 65535" | sudo tee /etc/security/limits.d/bench.conf
echo "* hard nofile 65535" | sudo tee -a /etc/security/limits.d/bench.conf
```

### 4. Machine A (server): deploy target
Build statically from your machine:
```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o chiserver-linux .
scp -i key.pem chiserver-linux ubuntu@<SERVER_PUBLIC_IP>:~/
ssh ubuntu@<SERVER_PUBLIC_IP> 'nohup ~/chiserver-linux > server.log 2>&1 &'
```

### 5. Machine B (generator): tools
```bash
sudo apt-get install -y wrk
go install github.com/rakyll/hey@latest
go install github.com/tsenart/vegeta/v12@latest
# k6: grab latest .deb from https://github.com/grafana/k6/releases
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o storm-linux ./cmd/storm
scp -i key.pem bench/{run_bench.sh,k6_script.js} ubuntu@<GEN_PUBLIC_IP>:~/bench/
```

### 6. Run
```bash
# on generator:
nohup bash ~/bench/run_bench.sh > ~/bench/suite.log 2>&1 &
```

### 7. Teardown (ALWAYS)
```bash
aws ec2 stop-instances --instance-ids <SERVER_ID> <GENERATOR_ID>
```
Compute charges stop; storage (~$0.10/day total) continues until volumes deleted.
