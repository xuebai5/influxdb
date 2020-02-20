package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCmdBucket(t *testing.T) {
	orgID := influxdb.ID(9000)

	fakeSVCFn := func(svc influxdb.BucketService) bucketSVCsFn {
		return func() (influxdb.BucketService, influxdb.OrganizationService, error) {
			return svc, &mock.OrganizationService{
				FindOrganizationF: func(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
					return &influxdb.Organization{ID: orgID, Name: "influxdata"}, nil
				},
			}, nil
		}
	}

	t.Run("create", func(t *testing.T) {
		tests := []struct {
			name           string
			expectedBucket influxdb.Bucket
			flags          []string
			envVars        map[string]string
		}{
			{
				name:  "basic just name",
				flags: []string{"--name=new name", "--org=org name"},
				expectedBucket: influxdb.Bucket{
					Name:  "new name",
					OrgID: orgID,
				},
			},
			{
				name: "with description and retention period",
				flags: []string{
					"--name=new name",
					"--description=desc",
					"--retention=1m",
					"--org=org name",
				},
				expectedBucket: influxdb.Bucket{
					Name:            "new name",
					Description:     "desc",
					RetentionPeriod: time.Minute,
					OrgID:           orgID,
				},
			},
			{
				name: "shorts",
				flags: []string{
					"-n=new name",
					"-d=desc",
					"-r=1m",
					"-o=org name",
				},
				expectedBucket: influxdb.Bucket{
					Name:            "new name",
					Description:     "desc",
					RetentionPeriod: time.Minute,
					OrgID:           orgID,
				},
			},
			{
				name: "env vars",
				flags: []string{
					"-d=desc",
					"-r=1m",
					"-o=org name",
				},
				envVars: map[string]string{"INFLUX_BUCKET_NAME": "new name"},
				expectedBucket: influxdb.Bucket{
					Name:            "new name",
					Description:     "desc",
					RetentionPeriod: time.Minute,
					OrgID:           orgID,
				},
			},
		}

		cmdFn := func(expectedBkt influxdb.Bucket) func(*globalFlags, genericCLIOpts) *cobra.Command {
			svc := mock.NewBucketService()
			svc.CreateBucketFn = func(ctx context.Context, bucket *influxdb.Bucket) error {
				if expectedBkt != *bucket {
					return fmt.Errorf("unexpected bucket;\n\twant= %+v\n\tgot=  %+v", expectedBkt, *bucket)
				}
				return nil
			}

			return func(g *globalFlags, opt genericCLIOpts) *cobra.Command {
				return newCmdBucketBuilder(fakeSVCFn(svc), opt).cmd()
			}
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				defer addEnvVars(t, tt.envVars)()

				builder := newInfluxCmdBuilder(
					in(new(bytes.Buffer)),
					out(ioutil.Discard),
				)
				cmd := builder.cmd(cmdFn(tt.expectedBucket))
				cmd.SetArgs(append([]string{"bucket", "create"}, tt.flags...))

				require.NoError(t, cmd.Execute())
			}

			t.Run(tt.name, fn)
		}
	})

	t.Run("delete", func(t *testing.T) {
		tests := []struct {
			name       string
			expectedID influxdb.ID
			flag       string
		}{
			{
				name:       "with description and retention period",
				expectedID: influxdb.ID(1),
				flag:       "--id=",
			},
			{
				name:       "shorts",
				expectedID: influxdb.ID(1),
				flag:       "-i=",
			},
		}

		cmdFn := func(expectedID influxdb.ID) func(*globalFlags, genericCLIOpts) *cobra.Command {
			svc := mock.NewBucketService()
			svc.FindBucketByIDFn = func(ctx context.Context, id influxdb.ID) (*influxdb.Bucket, error) {
				return &influxdb.Bucket{ID: id}, nil
			}
			svc.DeleteBucketFn = func(ctx context.Context, id influxdb.ID) error {
				if expectedID != id {
					return fmt.Errorf("unexpected id:\n\twant= %s\n\tgot=  %s", expectedID, id)
				}
				return nil
			}

			return func(g *globalFlags, opt genericCLIOpts) *cobra.Command {
				return newCmdBucketBuilder(fakeSVCFn(svc), opt).cmd()
			}
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				builder := newInfluxCmdBuilder(
					in(new(bytes.Buffer)),
					out(ioutil.Discard),
				)

				cmd := builder.cmd(cmdFn(tt.expectedID))
				idFlag := tt.flag + tt.expectedID.String()
				cmd.SetArgs([]string{"bucket", "find", idFlag})

				require.NoError(t, cmd.Execute())
			}

			t.Run(tt.name, fn)
		}
	})

	t.Run("find", func(t *testing.T) {
		type called struct {
			name  string
			id    influxdb.ID
			orgID influxdb.ID
			org   string
		}

		tests := []struct {
			name     string
			expected called
			flags    []string
			envVars  map[string]string
		}{
			{
				name:     "org id",
				flags:    []string{"--org-id=" + influxdb.ID(3).String()},
				envVars:  envVarsZeroMap,
				expected: called{orgID: 3},
			},
			{
				name: "id",
				flags: []string{
					"--id=" + influxdb.ID(2).String(),
					"--org-id=" + influxdb.ID(3).String(),
				},
				envVars: envVarsZeroMap,
				expected: called{
					id:    2,
					orgID: 3,
				},
			},
			{
				name:     "org",
				flags:    []string{"--org=rg"},
				envVars:  envVarsZeroMap,
				expected: called{org: "rg"},
			},
			{
				name:     "name",
				flags:    []string{"--org=rg", "--name=name1"},
				envVars:  envVarsZeroMap,
				expected: called{org: "rg", name: "name1"},
			},
			{
				name: "shorts",
				flags: []string{
					"-o=rg",
					"-n=name1",
					"-i=" + influxdb.ID(1).String(),
				},
				envVars:  envVarsZeroMap,
				expected: called{org: "rg", name: "name1", id: 1},
			},
			{
				name: "env vars",
				envVars: map[string]string{
					"INFLUX_ORG":         "rg",
					"INFLUX_BUCKET_NAME": "name1",
				},
				flags:    []string{"-i=" + influxdb.ID(1).String()},
				expected: called{org: "rg", name: "name1", id: 1},
			},
			{
				name: "env vars 2",
				envVars: map[string]string{
					"INFLUX_ORG":         "",
					"INFLUX_ORG_ID":      influxdb.ID(2).String(),
					"INFLUX_BUCKET_NAME": "name1",
				},
				flags:    []string{"-i=" + influxdb.ID(1).String()},
				expected: called{orgID: 2, name: "name1", id: 1},
			},
		}

		cmdFn := func() (func(*globalFlags, genericCLIOpts) *cobra.Command, *called) {
			calls := new(called)

			svc := mock.NewBucketService()
			svc.FindBucketsFn = func(ctx context.Context, f influxdb.BucketFilter, opt ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error) {
				if f.ID != nil {
					calls.id = *f.ID
				}
				if f.OrganizationID != nil {
					calls.orgID = *f.OrganizationID
				}
				if f.Name != nil {
					calls.name = *f.Name
				}
				if f.Org != nil {
					calls.org = *f.Org
				}
				return nil, 0, nil
			}

			return func(g *globalFlags, opt genericCLIOpts) *cobra.Command {
				return newCmdBucketBuilder(fakeSVCFn(svc), opt).cmd()
			}, calls
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				defer addEnvVars(t, tt.envVars)()

				builder := newInfluxCmdBuilder(
					in(new(bytes.Buffer)),
					out(ioutil.Discard),
				)

				cmdFn, calls := cmdFn()
				cmd := builder.cmd(cmdFn)

				cmd.SetArgs(append([]string{"bucket", "find"}, tt.flags...))

				require.NoError(t, cmd.Execute())
				assert.Equal(t, tt.expected, *calls)
			}

			t.Run(tt.name, fn)
		}
	})

	t.Run("update", func(t *testing.T) {
		tests := []struct {
			name     string
			expected influxdb.BucketUpdate
			flags    []string
			envVars  map[string]string
		}{
			{
				name: "basic just name",
				flags: []string{
					"--id=" + influxdb.ID(3).String(),
					"--name=new name",
				},
				expected: influxdb.BucketUpdate{
					Name: strPtr("new name"),
				},
			},
			{
				name: "with all fields",
				flags: []string{
					"--id=" + influxdb.ID(3).String(),
					"--name=new name",
					"--description=desc",
					"--retention=1m",
				},
				expected: influxdb.BucketUpdate{
					Name:            strPtr("new name"),
					Description:     strPtr("desc"),
					RetentionPeriod: durPtr(time.Minute),
				},
			},
			{
				name: "shorts",
				flags: []string{
					"-i=" + influxdb.ID(3).String(),
					"-n=new name",
					"-d=desc",
					"-r=1m",
				},
				expected: influxdb.BucketUpdate{
					Name:            strPtr("new name"),
					Description:     strPtr("desc"),
					RetentionPeriod: durPtr(time.Minute),
				},
			},
			{
				name: "env var",
				flags: []string{
					"-i=" + influxdb.ID(3).String(),
					"-d=desc",
					"-r=1m",
				},
				envVars: map[string]string{"INFLUX_BUCKET_NAME": "new name"},
				expected: influxdb.BucketUpdate{
					Name:            strPtr("new name"),
					Description:     strPtr("desc"),
					RetentionPeriod: durPtr(time.Minute),
				},
			},
		}

		cmdFn := func(expectedUpdate influxdb.BucketUpdate) func(*globalFlags, genericCLIOpts) *cobra.Command {
			svc := mock.NewBucketService()
			svc.UpdateBucketFn = func(ctx context.Context, id influxdb.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
				if id != 3 {
					return nil, fmt.Errorf("unexpecte id:\n\twant= %s\n\tgot=  %s", influxdb.ID(3), id)
				}
				if !reflect.DeepEqual(expectedUpdate, upd) {
					return nil, fmt.Errorf("unexpected bucket update;\n\twant= %+v\n\tgot=  %+v", expectedUpdate, upd)
				}
				return &influxdb.Bucket{}, nil
			}

			return func(g *globalFlags, opt genericCLIOpts) *cobra.Command {
				return newCmdBucketBuilder(fakeSVCFn(svc), opt).cmd()
			}
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				defer addEnvVars(t, tt.envVars)()

				builder := newInfluxCmdBuilder(
					in(new(bytes.Buffer)),
					out(ioutil.Discard),
				)

				cmd := builder.cmd(cmdFn(tt.expected))

				cmd.SetArgs(append([]string{"bucket", "update"}, tt.flags...))
				require.NoError(t, cmd.Execute())
			}

			t.Run(tt.name, fn)
		}
	})
}

func strPtr(s string) *string {
	return &s
}

func durPtr(d time.Duration) *time.Duration {
	return &d
}

func addEnvVars(t *testing.T, envVars map[string]string) func() {
	t.Helper()

	var initialEnvVars []struct{ key, val string }
	for key, val := range envVars {
		if k := os.Getenv(key); k != "" {
			initialEnvVars = append(initialEnvVars, struct{ key, val string }{
				key: key,
				val: k,
			})
		}

		require.NoError(t, os.Setenv(key, val))
	}
	return func() {
		for key := range envVars {
			require.NoError(t, os.Unsetenv(key))
		}

		for _, envVar := range initialEnvVars {
			require.NoError(t, os.Setenv(envVar.key, envVar.val))
		}
	}
}
