# Copy to scenario-cloud.settings.sh (gitignored) and fill in your account's values.
AMI_ID="ami-..."              # Ubuntu, x86_64
KEY_NAME="..."                # EC2 key pair name
PEM_PATH="C:/Users/<you>/Downloads/<key>.pem"
SECURITY_GROUP_ID="sg-..."    # must allow SSH (22) from this PC
SUBNET_ID="subnet-..."        # public subnet (instances need a public DNS name)
