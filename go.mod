module github.com/noctarius/timescaledb-event-streamer

go 1.21

require (
	github.com/BurntSushi/toml v1.3.2
	github.com/IBM/sarama v1.42.1
	github.com/antonmedv/expr v1.15.5
	github.com/aws/aws-sdk-go v1.50.1
	github.com/cenkalti/backoff/v4 v4.2.1
	github.com/docker/docker v24.0.7+incompatible
	github.com/go-errors/errors v1.5.1
	github.com/go-redis/redis v6.15.9+incompatible
	github.com/goccy/go-json v0.10.2
	github.com/gookit/color v1.5.4
	github.com/gookit/goutil v0.6.14
	github.com/gookit/slog v0.5.4
	github.com/hashicorp/go-uuid v1.0.3
	github.com/inhies/go-bytesize v0.0.0-20220417184213-4913239db9cf
	github.com/jackc/pgerrcode v0.0.0-20220416144525-469b46aa5efa
	github.com/jackc/pgio v1.0.0
	github.com/jackc/pglogrepl v0.0.0-20230810221841-d0818e1fbef7
	github.com/jackc/pgx/v5 v5.5.0
	github.com/nats-io/nats.go v1.31.0
	github.com/samber/do v1.6.0
	github.com/samber/lo v1.39.0
	github.com/segmentio/stats/v4 v4.1.0
	github.com/stretchr/testify v1.8.4
	github.com/testcontainers/testcontainers-go v0.26.0
	github.com/testcontainers/testcontainers-go/modules/localstack v0.26.0
	github.com/testcontainers/testcontainers-go/modules/redis v0.26.0
	github.com/twpayne/go-geom v1.5.3
	github.com/urfave/cli v1.22.14
	golang.org/x/exp v0.0.0-20230713183714-613f0c0eb8a1
	golang.org/x/net v0.19.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	dario.cat/mergo v1.0.0 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20230124172434-306776ec8161 // indirect
	github.com/Microsoft/go-winio v0.6.1 // indirect
	github.com/Microsoft/hcsshim v0.11.1 // indirect
	github.com/containerd/containerd v1.7.7 // indirect
	github.com/containerd/log v0.1.0 // indirect
	github.com/cpuguy83/dockercfg v0.3.1 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/docker/distribution v2.8.2+incompatible // indirect
	github.com/docker/go-connections v0.4.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/eapache/go-resiliency v1.4.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20230731223053-c322873962e3 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/google/uuid v1.3.1 // indirect
	github.com/gookit/gsr v0.1.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.4 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/josharian/native v1.1.0 // indirect
	github.com/klauspost/compress v1.17.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/magiconair/properties v1.8.7 // indirect
	github.com/mdlayher/genetlink v1.3.2 // indirect
	github.com/mdlayher/netlink v1.7.2 // indirect
	github.com/mdlayher/socket v0.4.1 // indirect
	github.com/mdlayher/taskstats v0.0.0-20230712191918-387b3d561d14 // indirect
	github.com/moby/patternmatcher v0.6.0 // indirect
	github.com/moby/sys/sequential v0.5.0 // indirect
	github.com/moby/term v0.5.0 // indirect
	github.com/morikuni/aec v1.0.0 // indirect
	github.com/nats-io/nkeys v0.4.5 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/onsi/ginkgo v1.16.5 // indirect
	github.com/onsi/gomega v1.27.6 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.0-rc5 // indirect
	github.com/opencontainers/runc v1.1.7 // indirect
	github.com/pierrec/lz4/v4 v4.1.18 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/rogpeppe/go-internal v1.9.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/segmentio/fasthash v1.0.3 // indirect
	github.com/shirou/gopsutil/v3 v3.23.9 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/stretchr/objx v0.5.0 // indirect
	github.com/tklauser/go-sysconf v0.3.12 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/xo/terminfo v0.0.0-20220910002029-abceb7e1c41e // indirect
	github.com/yusufpapurcu/wmi v1.2.3 // indirect
	golang.org/x/crypto v0.16.0 // indirect
	golang.org/x/mod v0.12.0 // indirect
	golang.org/x/sync v0.4.0 // indirect
	golang.org/x/sys v0.15.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	golang.org/x/tools v0.11.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230717213848-3f92550aa753 // indirect
	google.golang.org/grpc v1.57.1 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
)

replace github.com/segmentio/stats/v4 v4.1.0 => github.com/noctarius/segmentio_stats/v4 v4.1.5
