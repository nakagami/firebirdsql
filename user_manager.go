package firebirdsql

type User struct {
	Username   *string
	Password   *string
	FirstName  *string
	MiddleName *string
	LastName   *string
	UserId     int32
	GroupId    int32
	Admin      bool
}

type UserManager struct {
	sm         *ServiceManager
	securityDb string
}

type UserManagerOptions struct {
	ServiceManagerOptions
	SecurityDB string
}

func GetDefaultUserManagerOptions() UserManagerOptions {
	return UserManagerOptions{
		ServiceManagerOptions: GetDefaultServiceManagerOptions(),
		SecurityDB:            "",
	}
}

func (umo UserManagerOptions) WithoutWireCrypt() UserManagerOptions {
	umo.WireCrypt = false
	return umo
}

func (umo UserManagerOptions) WithWireCrypt() UserManagerOptions {
	umo.WireCrypt = true
	return umo
}

func (umo UserManagerOptions) WithAuthPlugin(authPlugin string) UserManagerOptions {
	umo.AuthPlugin = authPlugin
	return umo
}

func (umo UserManagerOptions) WithSecurityDB(securityDB string) UserManagerOptions {
	umo.SecurityDB = securityDB
	return umo
}

func NewUser(username string) *User {
	return &User{
		Username: &username,
		UserId:   -1,
		GroupId:  -1,
	}
}

func (u *User) WithPassword(password string) *User {
	u.Password = &password
	return u
}

func (u *User) WithFirstName(firstname string) *User {
	u.FirstName = &firstname
	return u
}

func (u *User) WithMiddleName(middlename string) *User {
	u.MiddleName = &middlename
	return u
}

func (u *User) WithLastName(lastname string) *User {
	u.LastName = &lastname
	return u
}

func (u *User) WithUserId(userId int32) *User {
	u.UserId = userId
	return u
}

func (u *User) WithGroupId(groupId int32) *User {
	u.GroupId = groupId
	return u
}

func (u *User) GetSpb() []byte {
	srb := NewXPBWriter()
	if u.Username != nil {
		srb.PutString(isc_spb_sec_username, *u.Username)
	}
	if u.Password != nil {
		srb.PutString(isc_spb_sec_password, *u.Password)
	}
	if u.FirstName != nil {
		srb.PutString(isc_spb_sec_firstname, *u.FirstName)
	}
	if u.MiddleName != nil {
		srb.PutString(isc_spb_sec_middlename, *u.MiddleName)
	}
	if u.LastName != nil {
		srb.PutString(isc_spb_sec_lastname, *u.LastName)
	}
	if u.UserId != -1 {
		srb.PutInt32(isc_spb_sec_userid, u.UserId)
	}
	if u.GroupId != -1 {
		srb.PutInt32(isc_spb_sec_groupid, u.GroupId)
	}
	return srb.Bytes()
}

func NewUserManager(addr string, user string, password string, options UserManagerOptions) (*UserManager, error) {
	var (
		sm  *ServiceManager
		err error
	)

	if sm, err = NewServiceManager(addr, user, password, options.ServiceManagerOptions); err != nil {
		return nil, err
	}
	return &UserManager{
		sm,
		options.SecurityDB,
	}, nil
}

func (um *UserManager) Close() error {
	return um.sm.Close()
}

func (um *UserManager) userAction(action byte, user *User) error {
	spb := NewXPBWriterFromTag(action)
	if user != nil {
		spb.PutBytes(user.GetSpb())
	}
	if um.securityDb != "" {
		spb.PutString(isc_spb_dbname, um.securityDb)
	}
	return um.sm.ServiceStart(spb.Bytes())
}

func (um *UserManager) AddUser(user *User) error {
	err := um.userAction(isc_action_svc_add_user, user)
	return err
}

func (um *UserManager) DeleteUser(user *User) error {
	del := NewUser(*user.Username)
	err := um.userAction(isc_action_svc_delete_user, del)
	return err
}

func (um *UserManager) ModifyUser(user *User) error {
	err := um.userAction(isc_action_svc_modify_user, user)
	return err
}

func (um *UserManager) GetUsers() ([]User, error) {
	var (
		err     error
		buf     []byte
		resChan = make(chan []byte)
		done    = make(chan bool)
		cont    = true
		users   []User
	)
	if err = um.userAction(isc_action_svc_display_user_adm, nil); err != nil {
		return nil, err
	}

	go func() {
		err = um.sm.WaitBuffer(resChan)
		done <- true
	}()

	for cont {
		select {
		case buf = <-resChan:
			srb := NewXPBReader(buf)
			var (
				user *User
				have bool
				val  byte
			)
			for {
				if have, val = srb.Next(); !have {
					break
				}
				switch val {
				case isc_spb_sec_username:
					if user != nil {
						users = append(users, *user)
					}
					user = NewUser(srb.GetString())
				case isc_spb_sec_firstname:
					s := srb.GetString()
					user.FirstName = &s
				case isc_spb_sec_middlename:
					s := srb.GetString()
					user.MiddleName = &s
				case isc_spb_sec_lastname:
					s := srb.GetString()
					user.MiddleName = &s
				case isc_spb_sec_userid:
					user.UserId = srb.GetInt32()
				case isc_spb_sec_groupid:
					user.GroupId = srb.GetInt32()
				case isc_spb_sec_admin:
					user.Admin = srb.GetInt16() > 0
				}
			}
			if user != nil {
				users = append(users, *user)
			}
		case <-done:
			cont = false
		}
	}

	return users, err
}
