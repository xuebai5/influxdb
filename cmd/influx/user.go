package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/http"
	"github.com/spf13/cobra"
	input "github.com/tcnksm/go-input"
)

type userSVCsFn func() (
	cmdUserDeps,
	error,
)

type cmdUserDeps struct {
	userSVC   influxdb.UserService
	orgSvc    influxdb.OrganizationService
	passSVC   influxdb.PasswordsService
	urmSVC    influxdb.UserResourceMappingService
	getPassFn func(*input.UI, bool) string
}

func cmdUser(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	builder := newCmdUserBuilder(newUserSVC, opt)
	builder.globalFlags = f
	return builder.cmd()
}

type cmdUserBuilder struct {
	genericCLIOpts
	*globalFlags

	svcFn userSVCsFn

	id       string
	name     string
	password string
	org      organization
}

func newCmdUserBuilder(svcsFn userSVCsFn, opt genericCLIOpts) *cmdUserBuilder {
	return &cmdUserBuilder{
		genericCLIOpts: opt,
		svcFn:          svcsFn,
	}
}

func (b *cmdUserBuilder) cmd() *cobra.Command {
	cmd := b.newCmd("user", nil)
	cmd.Short = "User management commands"
	cmd.Run = seeHelp
	cmd.AddCommand(
		b.cmdCreate(),
		b.cmdDelete(),
		b.cmdFind(),
		b.cmdUpdate(),
		b.cmdPassword(),
	)

	return cmd
}

func (b *cmdUserBuilder) cmdPassword() *cobra.Command {
	cmd := b.newCmd("password", b.cmdPasswordRunEFn)
	cmd.Short = "Update user password"

	cmd.Flags().StringVarP(&b.id, "id", "i", "", "The user ID")
	cmd.Flags().StringVarP(&b.name, "name", "n", "", "The user name")

	return cmd
}

func (b *cmdUserBuilder) cmdUpdate() *cobra.Command {
	cmd := b.newCmd("update", b.cmdUpdateRunEFn)
	cmd.Short = "Update user"

	cmd.Flags().StringVarP(&b.id, "id", "i", "", "The user ID (required)")
	cmd.Flags().StringVarP(&b.name, "name", "n", "", "The user name")
	cmd.MarkFlagRequired("id")

	return cmd
}

func newUserService() (influxdb.UserService, error) {
	if flags.local {
		return newLocalKVService()
	}

	client, err := newHTTPClient()
	if err != nil {
		return nil, err
	}
	return &http.UserService{
		Client: client,
	}, nil
}

func newUserSVC() (
	cmdUserDeps,
	error) {
	httpClient, err := newHTTPClient()
	if err != nil {
		return cmdUserDeps{}, err
	}
	userSvc := &http.UserService{Client: httpClient}
	orgSvc := &http.OrganizationService{Client: httpClient}
	passSvc := &http.PasswordService{Client: httpClient}
	urmSvc := &http.UserResourceMappingService{Client: httpClient}
	getPassFn := getPassword

	return cmdUserDeps{
		userSVC:   userSvc,
		orgSvc:    orgSvc,
		passSVC:   passSvc,
		urmSVC:    urmSvc,
		getPassFn: getPassFn,
	}, nil
}

func (b *cmdUserBuilder) cmdPasswordRunEFn(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	dep, err := b.svcFn()
	if err != nil {
		return err
	}

	filter := influxdb.UserFilter{}
	if b.name != "" {
		filter.Name = &b.name
	}
	if b.id != "" {
		id, err := influxdb.IDFromString(b.id)
		if err != nil {
			return err
		}
		filter.ID = id
	}
	u, err := dep.userSVC.FindUser(ctx, filter)
	if err != nil {
		return err
	}
	ui := &input.UI{
		Writer: b.genericCLIOpts.w,
		Reader: b.genericCLIOpts.in,
	}
	password := dep.getPassFn(ui, true)

	if err = dep.passSVC.SetPassword(ctx, u.ID, password); err != nil {
		return err
	}
	fmt.Fprintln(b.w, "Your password has been successfully updated.")
	return nil
}

func (b *cmdUserBuilder) cmdUpdateRunEFn(cmd *cobra.Command, args []string) error {
	dep, err := b.svcFn()
	if err != nil {
		return err
	}

	var id influxdb.ID
	if err := id.DecodeFromString(b.id); err != nil {
		return err
	}

	update := influxdb.UserUpdate{}
	if b.name != "" {
		update.Name = &b.name
	}

	user, err := dep.userSVC.UpdateUser(context.Background(), id, update)
	if err != nil {
		return err
	}

	w := b.newTabWriter()
	w.WriteHeaders(
		"ID",
		"Name",
	)
	w.Write(map[string]interface{}{
		"ID":   user.ID.String(),
		"Name": user.Name,
	})
	w.Flush()

	return nil
}

func (b *cmdUserBuilder) cmdCreate() *cobra.Command {
	cmd := b.newCmd("create", b.cmdCreateRunEFn)
	cmd.Short = "Create user"

	opts := flagOpts{
		{
			DestP:    &b.name,
			Flag:     "name",
			Short:    'n',
			Desc:     "The user name (required)",
			Required: true,
		},
	}
	opts.mustRegister(cmd)

	cmd.Flags().StringVarP(&b.password, "password", "p", "", "The user password")
	b.org.register(cmd, false)

	return cmd
}

func (b *cmdUserBuilder) cmdCreateRunEFn(*cobra.Command, []string) error {
	ctx := context.Background()
	if err := b.org.validOrgFlags(); err != nil {
		return err
	}

	dep, err := b.svcFn()
	if err != nil {
		return err
	}

	user := &influxdb.User{
		Name: b.name,
	}

	if err := dep.userSVC.CreateUser(ctx, user); err != nil {
		return err
	}

	writeOutput := func(headers []string, vals ...string) error {
		if len(headers) != len(vals) {
			return errors.New("invalid headers and val setup for writer")
		}

		m := make(map[string]interface{})
		for i, h := range headers {
			m[h] = vals[i]
		}
		w := b.newTabWriter()
		w.WriteHeaders(headers...)
		w.Write(m)
		w.Flush()

		return nil
	}

	orgID, err := b.org.getID(dep.orgSvc)
	if err != nil {
		return err
	}

	pass := b.password
	if orgID == 0 && pass == "" {
		return writeOutput([]string{"ID", "Name"}, user.ID.String(), user.Name)
	}

	if pass != "" && orgID == 0 {
		return errors.New("an org id is required when providing a user password")
	}

	err = dep.urmSVC.CreateUserResourceMapping(context.Background(), &influxdb.UserResourceMapping{
		UserID:       user.ID,
		UserType:     influxdb.Member,
		ResourceType: influxdb.OrgsResourceType,
		ResourceID:   orgID,
	})
	if err != nil {
		return err
	}

	if err := dep.passSVC.SetPassword(ctx, user.ID, pass); err != nil {
		return err
	}

	return writeOutput([]string{"ID", "Name", "Organization ID"}, user.ID.String(), user.Name, orgID.String())
}

func (b *cmdUserBuilder) cmdFind() *cobra.Command {
	cmd := b.newCmd("find", b.cmdFindRunEFn)
	cmd.Short = "Find user"

	cmd.Flags().StringVarP(&b.id, "id", "i", "", "The user ID")
	cmd.Flags().StringVarP(&b.name, "name", "n", "", "The user name")

	return cmd
}

func (b *cmdUserBuilder) cmdFindRunEFn(*cobra.Command, []string) error {
	dep, err := b.svcFn()
	if err != nil {
		return err
	}

	filter := influxdb.UserFilter{}
	if b.name != "" {
		filter.Name = &b.name
	}
	if b.id != "" {
		id, err := influxdb.IDFromString(b.id)
		if err != nil {
			return err
		}
		filter.ID = id
	}

	users, _, err := dep.userSVC.FindUsers(context.Background(), filter)
	if err != nil {
		return err
	}

	w := b.newTabWriter()
	w.WriteHeaders(
		"ID",
		"Name",
	)
	for _, u := range users {
		w.Write(map[string]interface{}{
			"ID":   u.ID.String(),
			"Name": u.Name,
		})
	}
	w.Flush()

	return nil
}

func (b *cmdUserBuilder) cmdDelete() *cobra.Command {
	cmd := b.newCmd("delete", b.cmdDeleteRunEFn)
	cmd.Short = "Delete user"

	cmd.Flags().StringVarP(&b.id, "id", "i", "", "The user ID (required)")
	cmd.MarkFlagRequired("id")

	return cmd
}

func (b *cmdUserBuilder) cmdDeleteRunEFn(cmd *cobra.Command, args []string) error {
	dep, err := b.svcFn()
	if err != nil {
		return err
	}

	var id influxdb.ID
	if err := id.DecodeFromString(b.id); err != nil {
		return err
	}

	ctx := context.Background()
	u, err := dep.userSVC.FindUserByID(ctx, id)
	if err != nil {
		return err
	}

	if err := dep.userSVC.DeleteUser(ctx, id); err != nil {
		return err
	}

	w := b.newTabWriter()
	w.WriteHeaders(
		"ID",
		"Name",
		"Deleted",
	)
	w.Write(map[string]interface{}{
		"ID":      u.ID.String(),
		"Name":    u.Name,
		"Deleted": true,
	})
	w.Flush()

	return nil
}
