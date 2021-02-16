import copy

class Player(object):
    def __init__(self, csv_line):
        tokens = csv_line.strip().replace('"', '').split(',')
        self.contract_identity = tokens[0]
        self.account_email = tokens[1]
        self.contract_id = str(
            int(tokens[2].replace('+', '').replace('.', '')))
        self.email_verified_status = str(int(
            tokens[3].replace('+', '').replace('.', '')))
        self.pp_status = tokens[6].strip()
        self.sc_status = tokens[7].strip()

    def get_contract_identity(self): return self.contract_identity
    def set_contract_identity(self, contract_identity): self.contract_identity = contract_identity
    contractIdentity = property(get_contract_identity, set_contract_identity)

    def get_contract_id(self): return self.contract_id
    def set_contract_id(self, contract_id): self.contract_id = contract_id
    contractId = property(get_contract_id, set_contract_id)

    def get_username(self): return self.account_email
    def set_username(self, username): self.account_email = username
    username = property(get_username, set_username)

    def get_email_verified(self): return self.email_verified_status
    def set_email_verified(self, email_verified_status): self.email_verified_status = email_verified_status
    emailVerified = property(get_email_verified, set_email_verified)

    def get_pp_status(self): return self.pp_status
    def set_pp_status(self, pp_status): self.pp_status = pp_status
    portalService = property(get_pp_status, set_pp_status)

    def get_sc_status(self): return self.sc_status
    def set_sc_status(self, sc_status): self.sc_status = sc_status
    secondChanceService = property(get_sc_status, set_sc_status)

    def preactivate(self):
        self.email_verified_status = '0'
        self.pp_status = '1'
        self.sc_status = '1'

    def activate(self):
        self.email_verified_status = '1'
        self.pp_status = '2'
        self.sc_status = '2'

    def suspend(self):
        self.email_verified_status = '1'
        self.pp_status = '3'
        self.sc_status = '3'

    def __ne__(self, other):
        not_equal = False

        if type(self) != type(other):
            not_equal = True
        elif self.contract_identity != other.contract_identity:
            not_equal = True
        elif self.contract_id != other.contract_id:
            not_equal = True
        elif self.email_verified_status != other.email_verified_status:
            not_equal = True
        elif self.pp_status != other.pp_status:
            not_equal = True
        elif self.sc_status != other.sc_status:
            not_equal = True
        elif self.account_email != other.account_email:
            not_equal = True

        return not_equal

    def __eq__(self, other):
        is_equal = False

        if type(self) == type(other):
            if self.contract_identity == other.contract_identity and \
               self.contract_id == other.contract_id and \
               self.email_verified_status == other.email_verified_status and \
               self.pp_status == other.pp_status and \
               self.sc_status == other.sc_status and \
               self.account_email == other.account_email:
                is_equal = True

        return is_equal

    def __str__(self):
        format = '%s, %s, %s, %s, %s, %s'
        return format % (
            self.contract_identity,
            self.contract_id,
            self.email_verified_status,
            self.pp_status,
            self.sc_status,
            self.account_email
        )

def try_player():
    format = '%s, %s, %s, %s, %s, %s'
    print(format % (
        'contract_identity',
        'contract_id',
        'email_verified',
        'pp_service',
        'sc_service',
        'username'
    ))
    csv_file=open('scenarios.csv')
    for csv_line in csv_file:
        if csv_line.find('CONTRACT_IDENTITY') >= 0:
            continue  # Skip column-heading row.

        player = Player(csv_line)
        print(player)

def main():
    try_player()

if __name__ == "__main__":
    main()
