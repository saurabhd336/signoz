package pinotReader

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/oklog/pkg/group"
	"github.com/pkg/errors"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	sd_config "github.com/prometheus/prometheus/discovery/config"
	"github.com/prometheus/prometheus/promql"

	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/util/stats"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jmoiron/sqlx"

	promModel "github.com/prometheus/common/model"
	"go.signoz.io/query-service/constants"
	am "go.signoz.io/query-service/integrations/alertManager"
	"go.signoz.io/query-service/model"
	"go.signoz.io/query-service/utils"
	"go.uber.org/zap"
)

const (
	primaryNamespace      = "clickhouse"
	archiveNamespace      = "clickhouse-archive"
	signozTraceDBName     = "signoz_traces"
	signozDurationMVTable = "durationSort"
	signozSpansTable      = "signoz_spans"
	signozErrorIndexTable = "signoz_error_index_v2"
	signozTraceTableName  = "signoz_index_v2"
	signozMetricDBName    = "signoz_metrics"
	signozSampleTableName = "samples_v2"
	signozTSTableName     = "time_series_v2"

	minTimespanForProgressiveSearch       = time.Hour
	minTimespanForProgressiveSearchMargin = time.Minute
	maxProgressiveSteps                   = 4
	charset                               = "abcdefghijklmnopqrstuvwxyz" +
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	DEFAULT_UNLIMITED_LIMIT = "999999999"
)

var (
	ErrNoOperationsTable            = errors.New("no operations table supplied")
	ErrNoIndexTable                 = errors.New("no index table supplied")
	ErrStartTimeRequired            = errors.New("start time is required for search queries")
	seededRand           *rand.Rand = rand.New(
		rand.NewSource(time.Now().UnixNano()))
)

type PinotClient struct {
	// This is where we need to provide the APIs
	datasource string
}

func (r *PinotClient) ControllerQuery(ctx context.Context, query string) (PinotResponseTable, error) {
	zap.S().Info("Querying Pinot: %s", query)
	sqlReq := map[string]string{"sql": query}
	jsonReq, err := json.Marshal(sqlReq)

	if err != nil {
		zap.S().Error("Json marshilling failed", err)
	}

	resp, reqErr := http.Post(r.datasource, "application/json", bytes.NewBuffer(jsonReq))

	if reqErr != nil {
		zap.S().Error("request failed", reqErr)
	}
	defer resp.Body.Close()

	var res map[string]interface{}

	json.NewDecoder(resp.Body).Decode(&res)
	rows := res["resultTable"].(map[string]interface{})["rows"].([]interface{})

	return PinotResponseTable{rows: rows}, nil
}

func (r *PinotClient) Query(ctx context.Context, query string) (PinotRows, error) {
	// Actual queries to be executed here
	sqlReq := map[string]string{"sql": query}
	jsonReq, err := json.Marshal(sqlReq)

	if err != nil {
		zap.S().Error("Json marshilling failed", err)
	}

	resp, reqErr := http.Post("http://localhost:9000/sql", "application/json", bytes.NewBuffer(jsonReq))

	if reqErr != nil {
		zap.S().Error("request failed", reqErr)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		zap.S().Error(err)
	}
	bodyString := string(bodyBytes)

	zap.S().Error("Response : '%v'", bodyString)

	return nil, nil
}

func (r *PinotClient) Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	zap.S().Error("Called Pinot with query: %v", query)

	return nil
}

func (r *PinotClient) QueryRow(ctx context.Context, query string, args ...interface{}) PinotRows {
	zap.S().Error("Called Pinot with query: %v", query)

	return nil
}

type PinotResponseTable struct {
	columns []string
	rows    []interface{}
}

type PinotRows interface {
	Next() bool
	Scan(dest ...interface{}) error
	ColumnTypes() []PinotColumnType
	Columns() []string
	Close() error
}

type PinotColumnType interface {
	Name() string
	Nullable() bool
	ScanType() reflect.Type
	DatabaseTypeName() string
}

// SpanWriter for reading spans from ClickHouse
type PinotReader struct {
	db PinotClient
	// These tables will be created in Pinot
	traceDB         string
	operationsTable string
	durationTable   string
	indexTable      string
	errorTable      string
	spansTable      string

	localDB       *sqlx.DB
	queryEngine   *promql.Engine
	alertManager  am.Manager
	remoteStorage *remote.Storage

	promConfigFile string
	promConfig     *config.Config
}

// NewTraceReader returns a TraceReader for the database
func NewReader(localDB *sqlx.DB, configFile string) *PinotReader {
	datasource := os.Getenv("PinotControllerUrl")
	pinotClient := PinotClient{
		datasource: datasource,
	}

	alertManager, err := am.New("")
	if err != nil {
		zap.S().Errorf("msg: failed to initialize alert manager: ", "/t error:", err)
		zap.S().Errorf("msg: check if the alert manager URL is correctly set and valid")
		os.Exit(1)
	}

	return &PinotReader{
		db:           pinotClient,
		localDB:      localDB,
		alertManager: alertManager,
		traceDB:      signozTraceDBName,
		indexTable:   signozTraceTableName,
		spansTable:   signozSpansTable,
		errorTable:   signozErrorIndexTable,
	}
}

func (r *PinotReader) Start() {
	logLevel := promlog.AllowedLevel{}
	logLevel.Set("debug")
	// allowedFormat := promlog.AllowedFormat{}
	// allowedFormat.Set("logfmt")

	// promlogConfig := promlog.Config{
	// 	Level:  &logLevel,
	// 	Format: &allowedFormat,
	// }

	logger := promlog.New(logLevel)

	startTime := func() (int64, error) {
		return int64(promModel.Latest), nil
	}

	remoteStorage := remote.NewStorage(log.With(logger, "component", "remote"), startTime, time.Duration(1*time.Minute))

	cfg := struct {
		configFile string

		localStoragePath    string
		lookbackDelta       promModel.Duration
		webTimeout          promModel.Duration
		queryTimeout        promModel.Duration
		queryConcurrency    int
		queryMaxSamples     int
		RemoteFlushDeadline promModel.Duration

		prometheusURL string

		logLevel promlog.AllowedLevel
	}{
		configFile: r.promConfigFile,
	}

	// fanoutStorage := remoteStorage
	fanoutStorage := storage.NewFanout(logger, remoteStorage)

	ctxScrape, cancelScrape := context.WithCancel(context.Background())
	discoveryManagerScrape := discovery.NewManager(ctxScrape, log.With(logger, "component", "discovery manager scrape"), discovery.Name("scrape"))

	scrapeManager := scrape.NewManager(log.With(logger, "component", "scrape manager"), fanoutStorage)

	opts := promql.EngineOpts{
		Logger:        log.With(logger, "component", "query engine"),
		Reg:           nil,
		MaxConcurrent: 20,
		MaxSamples:    50000000,
		Timeout:       time.Duration(2 * time.Minute),
	}

	queryEngine := promql.NewEngine(opts)

	reloaders := []func(cfg *config.Config) error{
		remoteStorage.ApplyConfig,
		// The Scrape managers need to reload before the Discovery manager as
		// they need to read the most updated config when receiving the new targets list.
		scrapeManager.ApplyConfig,
		func(cfg *config.Config) error {
			c := make(map[string]sd_config.ServiceDiscoveryConfig)
			for _, v := range cfg.ScrapeConfigs {
				c[v.JobName] = v.ServiceDiscoveryConfig
			}
			return discoveryManagerScrape.ApplyConfig(c)
		},
	}

	// sync.Once is used to make sure we can close the channel at different execution stages(SIGTERM or when the config is loaded).
	type closeOnce struct {
		C     chan struct{}
		once  sync.Once
		Close func()
	}
	// Wait until the server is ready to handle reloading.
	reloadReady := &closeOnce{
		C: make(chan struct{}),
	}
	reloadReady.Close = func() {
		reloadReady.once.Do(func() {
			close(reloadReady.C)
		})
	}

	var g group.Group
	{
		// Scrape discovery manager.
		g.Add(
			func() error {
				err := discoveryManagerScrape.Run()
				level.Info(logger).Log("msg", "Scrape discovery manager stopped")
				return err
			},
			func(err error) {
				level.Info(logger).Log("msg", "Stopping scrape discovery manager...")
				cancelScrape()
			},
		)
	}
	{
		// Scrape manager.
		g.Add(
			func() error {
				// When the scrape manager receives a new targets list
				// it needs to read a valid config for each job.
				// It depends on the config being in sync with the discovery manager so
				// we wait until the config is fully loaded.
				<-reloadReady.C

				err := scrapeManager.Run(discoveryManagerScrape.SyncCh())
				level.Info(logger).Log("msg", "Scrape manager stopped")
				return err
			},
			func(err error) {
				// Scrape manager needs to be stopped before closing the local TSDB
				// so that it doesn't try to write samples to a closed storage.
				level.Info(logger).Log("msg", "Stopping scrape manager...")
				scrapeManager.Stop()
			},
		)
	}
	{
		// Initial configuration loading.
		cancel := make(chan struct{})
		g.Add(
			func() error {
				// select {
				// case <-dbOpen:
				// 	break
				// // In case a shutdown is initiated before the dbOpen is released
				// case <-cancel:
				// 	reloadReady.Close()
				// 	return nil
				// }
				var err error
				r.promConfig, err = reloadConfig(cfg.configFile, logger, reloaders...)
				if err != nil {
					return fmt.Errorf("error loading config from %q: %s", cfg.configFile, err)
				}

				reloadReady.Close()

				// ! commented the alert manager can now
				// call query service to do this
				// channels, apiErrorObj := r.GetChannels()

				//if apiErrorObj != nil {
				//	zap.S().Errorf("Not able to read channels from DB")
				//}
				//for _, channel := range *channels {
				//apiErrorObj = r.LoadChannel(&channel)
				//if apiErrorObj != nil {
				//	zap.S().Errorf("Not able to load channel with id=%d loaded from DB", channel.Id, channel.Data)
				//}
				//}

				<-cancel

				return nil
			},
			func(err error) {
				close(cancel)
			},
		)
	}
	r.queryEngine = queryEngine
	r.remoteStorage = remoteStorage

	if err := g.Run(); err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}

}

func reloadConfig(filename string, logger log.Logger, rls ...func(*config.Config) error) (promConfig *config.Config, err error) {
	level.Info(logger).Log("msg", "Loading configuration file", "filename", filename)

	conf, err := config.LoadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("couldn't load configuration (--config.file=%q): %v", filename, err)
	}

	failed := false
	for _, rl := range rls {
		if err := rl(conf); err != nil {
			level.Error(logger).Log("msg", "Failed to apply configuration", "err", err)
			failed = true
		}
	}
	if failed {
		return nil, fmt.Errorf("one or more errors occurred while applying the new configuration (--config.file=%q)", filename)
	}
	level.Info(logger).Log("msg", "Completed loading of configuration file", "filename", filename)
	return conf, nil
}

func (r *PinotReader) GetConn() clickhouse.Conn {
	return nil
}

func (r *PinotReader) LoadChannel(channel *model.ChannelItem) *model.ApiError {

	receiver := &am.Receiver{}
	if err := json.Unmarshal([]byte(channel.Data), receiver); err != nil { // Parse []byte to go struct pointer
		return &model.ApiError{Typ: model.ErrorBadData, Err: err}
	}

	response, err := http.Post(constants.GetAlertManagerApiPrefix()+"v1/receivers", "application/json", bytes.NewBuffer([]byte(channel.Data)))

	if err != nil {
		zap.S().Errorf("Error in getting response of API call to alertmanager/v1/receivers\n", err)
		return &model.ApiError{Typ: model.ErrorInternal, Err: err}
	}
	if response.StatusCode > 299 {
		responseData, _ := ioutil.ReadAll(response.Body)

		err := fmt.Errorf("Error in getting 2xx response in API call to alertmanager/v1/receivers\n Status: %s \n Data: %s", response.Status, string(responseData))
		zap.S().Error(err)

		return &model.ApiError{Typ: model.ErrorInternal, Err: err}
	}

	return nil
}

func (r *PinotReader) GetChannel(id string) (*model.ChannelItem, *model.ApiError) {

	idInt, _ := strconv.Atoi(id)
	channel := model.ChannelItem{}

	query := fmt.Sprintf("SELECT id, created_at, updated_at, name, type, data data FROM notification_channels WHERE id=%d", idInt)

	err := r.localDB.Get(&channel, query)

	zap.S().Info(query)

	if err != nil {
		zap.S().Debug("Error in processing sql query: ", err)
		return nil, &model.ApiError{Typ: model.ErrorInternal, Err: err}
	}

	return &channel, nil

}

func (r *PinotReader) DeleteChannel(id string) *model.ApiError {

	idInt, _ := strconv.Atoi(id)

	channelToDelete, apiErrorObj := r.GetChannel(id)

	if apiErrorObj != nil {
		return apiErrorObj
	}

	tx, err := r.localDB.Begin()
	if err != nil {
		return &model.ApiError{Typ: model.ErrorInternal, Err: err}
	}

	{
		stmt, err := tx.Prepare(`DELETE FROM notification_channels WHERE id=$1;`)
		if err != nil {
			zap.S().Errorf("Error in preparing statement for INSERT to notification_channels\n", err)
			tx.Rollback()
			return &model.ApiError{Typ: model.ErrorInternal, Err: err}
		}
		defer stmt.Close()

		if _, err := stmt.Exec(idInt); err != nil {
			zap.S().Errorf("Error in Executing prepared statement for INSERT to notification_channels\n", err)
			tx.Rollback() // return an error too, we may want to wrap them
			return &model.ApiError{Typ: model.ErrorInternal, Err: err}
		}
	}

	apiError := r.alertManager.DeleteRoute(channelToDelete.Name)
	if apiError != nil {
		tx.Rollback()
		return apiError
	}

	err = tx.Commit()
	if err != nil {
		zap.S().Errorf("Error in committing transaction for DELETE command to notification_channels\n", err)
		return &model.ApiError{Typ: model.ErrorInternal, Err: err}
	}

	return nil

}

func (r *PinotReader) GetChannels() (*[]model.ChannelItem, *model.ApiError) {

	channels := []model.ChannelItem{}

	query := fmt.Sprintf("SELECT id, created_at, updated_at, name, type, data data FROM notification_channels")

	err := r.localDB.Select(&channels, query)

	zap.S().Info(query)

	if err != nil {
		zap.S().Debug("Error in processing sql query: ", err)
		return nil, &model.ApiError{Typ: model.ErrorInternal, Err: err}
	}

	return &channels, nil

}

func getChannelType(receiver *am.Receiver) string {

	if receiver.EmailConfigs != nil {
		return "email"
	}
	if receiver.OpsGenieConfigs != nil {
		return "opsgenie"
	}
	if receiver.PagerdutyConfigs != nil {
		return "pagerduty"
	}
	if receiver.PushoverConfigs != nil {
		return "pushover"
	}
	if receiver.SNSConfigs != nil {
		return "sns"
	}
	if receiver.SlackConfigs != nil {
		return "slack"
	}
	if receiver.VictorOpsConfigs != nil {
		return "victorops"
	}
	if receiver.WebhookConfigs != nil {
		return "webhook"
	}
	if receiver.WechatConfigs != nil {
		return "wechat"
	}

	return ""
}

func (r *PinotReader) EditChannel(receiver *am.Receiver, id string) (*am.Receiver, *model.ApiError) {

	idInt, _ := strconv.Atoi(id)

	channel, apiErrObj := r.GetChannel(id)

	if apiErrObj != nil {
		return nil, apiErrObj
	}
	if channel.Name != receiver.Name {
		return nil, &model.ApiError{Typ: model.ErrorBadData, Err: fmt.Errorf("channel name cannot be changed")}
	}

	tx, err := r.localDB.Begin()
	if err != nil {
		return nil, &model.ApiError{Typ: model.ErrorInternal, Err: err}
	}

	channel_type := getChannelType(receiver)
	receiverString, _ := json.Marshal(receiver)

	{
		stmt, err := tx.Prepare(`UPDATE notification_channels SET updated_at=$1, type=$2, data=$3 WHERE id=$4;`)

		if err != nil {
			zap.S().Errorf("Error in preparing statement for UPDATE to notification_channels\n", err)
			tx.Rollback()
			return nil, &model.ApiError{Typ: model.ErrorInternal, Err: err}
		}
		defer stmt.Close()

		if _, err := stmt.Exec(time.Now(), channel_type, string(receiverString), idInt); err != nil {
			zap.S().Errorf("Error in Executing prepared statement for UPDATE to notification_channels\n", err)
			tx.Rollback() // return an error too, we may want to wrap them
			return nil, &model.ApiError{Typ: model.ErrorInternal, Err: err}
		}
	}

	apiError := r.alertManager.EditRoute(receiver)
	if apiError != nil {
		tx.Rollback()
		return nil, apiError
	}

	err = tx.Commit()
	if err != nil {
		zap.S().Errorf("Error in committing transaction for INSERT to notification_channels\n", err)
		return nil, &model.ApiError{Typ: model.ErrorInternal, Err: err}
	}

	return receiver, nil

}

func (r *PinotReader) CreateChannel(receiver *am.Receiver) (*am.Receiver, *model.ApiError) {

	tx, err := r.localDB.Begin()
	if err != nil {
		return nil, &model.ApiError{Typ: model.ErrorInternal, Err: err}
	}

	channel_type := getChannelType(receiver)
	receiverString, _ := json.Marshal(receiver)

	// todo: check if the channel name already exists, raise an error if so

	{
		stmt, err := tx.Prepare(`INSERT INTO notification_channels (created_at, updated_at, name, type, data) VALUES($1,$2,$3,$4,$5);`)
		if err != nil {
			zap.S().Errorf("Error in preparing statement for INSERT to notification_channels\n", err)
			tx.Rollback()
			return nil, &model.ApiError{Typ: model.ErrorInternal, Err: err}
		}
		defer stmt.Close()

		if _, err := stmt.Exec(time.Now(), time.Now(), receiver.Name, channel_type, string(receiverString)); err != nil {
			zap.S().Errorf("Error in Executing prepared statement for INSERT to notification_channels\n", err)
			tx.Rollback() // return an error too, we may want to wrap them
			return nil, &model.ApiError{Typ: model.ErrorInternal, Err: err}
		}
	}

	apiError := r.alertManager.AddRoute(receiver)
	if apiError != nil {
		tx.Rollback()
		return nil, apiError
	}

	err = tx.Commit()
	if err != nil {
		zap.S().Errorf("Error in committing transaction for INSERT to notification_channels\n", err)
		return nil, &model.ApiError{Typ: model.ErrorInternal, Err: err}
	}

	return receiver, nil

}

func (r *PinotReader) GetInstantQueryMetricsResult(ctx context.Context, queryParams *model.InstantQueryMetricsParams) (*promql.Result, *stats.QueryStats, *model.ApiError) {
	return nil, nil, &model.ApiError{Typ: model.ErrorNotImplemented, Err: fmt.Errorf("Metrics not supported")}
}

func (r *PinotReader) GetQueryRangeResult(ctx context.Context, query *model.QueryRangeParams) (*promql.Result, *stats.QueryStats, *model.ApiError) {
	return nil, nil, &model.ApiError{Typ: model.ErrorNotImplemented, Err: fmt.Errorf("Metrics not supported")}
}

func (r *PinotReader) GetServicesList(ctx context.Context) (*[]string, error) {

	// TODO check the where clause support
	services := []string{}
	query := fmt.Sprintf(`SELECT DISTINCT serviceName FROM %s LIMIT %s`, r.indexTable, DEFAULT_UNLIMITED_LIMIT)

	rows, err := r.db.ControllerQuery(ctx, query)

	zap.S().Info(query)

	if err != nil {
		zap.S().Debug("Error in processing sql query: ", err)
		return nil, fmt.Errorf("Error in processing sql query")
	}

	for _, row := range rows.rows {
		_r := row.([]interface{})

		services = append(services, _r[0].(string))
	}

	return &services, nil
}

func (r *PinotReader) GetServices(ctx context.Context, queryParams *model.GetServicesParams) (*[]model.ServiceItem, *model.ApiError) {

	if r.indexTable == "" {
		return nil, &model.ApiError{Typ: model.ErrorExec, Err: ErrNoIndexTable}
	}

	serviceItems := []model.ServiceItem{}

	query := fmt.Sprintf("SELECT serviceName, percentileest(durationNano, 99) as p99, avg(durationNano) as avgDuration, count(*) as numCalls FROM %s WHERE \"timestamp\">=%s AND \"timestamp\"<=%s AND kind='2'", r.indexTable, strconv.FormatInt(queryParams.Start.UnixNano(), 10), strconv.FormatInt(queryParams.End.UnixNano(), 10))
	// args := []interface{}{}
	// args, errStatus := buildQueryWithTagParams(ctx, queryParams.Tags, &query, args)
	// if errStatus != nil {
	// 	return nil, errStatus
	// }
	query += " GROUP BY serviceName ORDER BY p99 DESC LIMIT " + DEFAULT_UNLIMITED_LIMIT
	rows, err := r.db.ControllerQuery(ctx, query)

	for _, row := range rows.rows {
		_r := row.([]interface{})

		serviceItems = append(serviceItems, model.ServiceItem{
			ServiceName:  _r[0].(string),
			Percentile99: _r[1].(float64),
			AvgDuration:  _r[2].(float64),
			NumCalls:     uint64(_r[3].(float64)),
		})
	}

	zap.S().Info(query)

	if err != nil {
		zap.S().Debug("Error in processing sql query: ", err)
		return nil, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query")}
	}

	//////////////////		Below block gets 5xx of services
	serviceErrorItems := []model.ServiceItem{}

	query = fmt.Sprintf("SELECT serviceName, count(*) as numErrors FROM %s WHERE \"timestamp\">=%s AND \"timestamp\"<=%s AND kind='2' AND (statusCode>=500 OR statusCode=2)", r.indexTable, strconv.FormatInt(queryParams.Start.UnixNano(), 10), strconv.FormatInt(queryParams.End.UnixNano(), 10))
	// args = []interface{}{}
	// args, errStatus = buildQueryWithTagParams(ctx, queryParams.Tags, &query, args)
	// if errStatus != nil {
	// 	return nil, errStatus
	// }
	query += " GROUP BY serviceName LIMIT " + DEFAULT_UNLIMITED_LIMIT
	rows, err = r.db.ControllerQuery(ctx, query)

	for _, row := range rows.rows {
		_r := row.([]interface{})

		serviceErrorItems = append(serviceErrorItems, model.ServiceItem{
			ServiceName: _r[0].(string),
			NumErrors:   uint64(_r[1].(float64)),
		})
	}

	zap.S().Info(query)

	if err != nil {
		zap.S().Debug("Error in processing sql query: ", err)
		return nil, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query")}
	}

	m5xx := make(map[string]uint64)

	for j := range serviceErrorItems {
		m5xx[serviceErrorItems[j].ServiceName] = serviceErrorItems[j].NumErrors
	}
	///////////////////////////////////////////

	//////////////////		Below block gets 4xx of services

	service4xxItems := []model.ServiceItem{}

	query = fmt.Sprintf("SELECT serviceName, count(*) as num4xx FROM %s WHERE \"timestamp\">=%s AND \"timestamp\"<=%s AND kind='2' AND statusCode>=400 AND statusCode<500", r.indexTable, strconv.FormatInt(queryParams.Start.UnixNano(), 10), strconv.FormatInt(queryParams.End.UnixNano(), 10))
	// args = []interface{}{}
	// args, errStatus = buildQueryWithTagParams(ctx, queryParams.Tags, &query, args)
	// if errStatus != nil {
	// 	return nil, errStatus
	// }
	query += " GROUP BY serviceName LIMIT " + DEFAULT_UNLIMITED_LIMIT
	rows, err = r.db.ControllerQuery(ctx, query)

	for _, row := range rows.rows {
		_r := row.([]interface{})

		service4xxItems = append(service4xxItems, model.ServiceItem{
			ServiceName: _r[0].(string),
			NumErrors:   uint64(_r[1].(float64)),
		})
	}

	zap.S().Info(query)

	if err != nil {
		zap.S().Debug("Error in processing sql query: ", err)
		return nil, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query")}
	}

	m4xx := make(map[string]uint64)

	for j := range service4xxItems {
		m4xx[service4xxItems[j].ServiceName] = service4xxItems[j].Num4XX
	}

	for i := range serviceItems {
		if val, ok := m5xx[serviceItems[i].ServiceName]; ok {
			serviceItems[i].NumErrors = val
		}
		if val, ok := m4xx[serviceItems[i].ServiceName]; ok {
			serviceItems[i].Num4XX = val
		}
		serviceItems[i].CallRate = float64(serviceItems[i].NumCalls) / float64(queryParams.Period)
		serviceItems[i].FourXXRate = float64(serviceItems[i].Num4XX) * 100 / float64(serviceItems[i].NumCalls)
		serviceItems[i].ErrorRate = float64(serviceItems[i].NumErrors) * 100 / float64(serviceItems[i].NumCalls)
	}

	return &serviceItems, nil
}

func (r *PinotReader) GetServiceOverview(ctx context.Context, queryParams *model.GetServiceOverviewParams) (*[]model.ServiceOverviewItem, *model.ApiError) {

	serviceOverviewItems := []model.ServiceOverviewItem{}

	query := fmt.Sprintf("SELECT DATETIMECONVERT(\"timestamp\", '1:NANOSECONDS:EPOCH', '1:MINUTES:EPOCH', '%s:MINUTES') AS \"time\", percentileest(durationNano, 50) as p50, percentileest(durationNano, 95) as p95, percentileest(durationNano, 99) as p99, count(*) as numCalls FROM %s WHERE \"timestamp\" >= %s AND \"timestamp\" <= %s AND kind = '2' AND serviceName = '%s'", strconv.Itoa(int(queryParams.StepSeconds/60)), r.indexTable, strconv.FormatInt(queryParams.Start.UnixNano(), 10), strconv.FormatInt(queryParams.End.UnixNano(), 10), queryParams.ServiceName)
	// args := []interface{}{}
	// args, errStatus := buildQueryWithTagParams(ctx, queryParams.Tags, &query, args)
	// if errStatus != nil {
	// 	return nil, errStatus
	// }
	query += " GROUP BY \"time\" ORDER BY \"time\" DESC LIMIT " + DEFAULT_UNLIMITED_LIMIT
	rows, err := r.db.ControllerQuery(ctx, query)

	for _, row := range rows.rows {
		_r := row.([]interface{})

		serviceOverviewItems = append(serviceOverviewItems, model.ServiceOverviewItem{
			Time:         time.Unix(0, int64(_r[0].(float64))*int64(time.Minute)),
			Percentile50: _r[1].(float64),
			Percentile95: _r[2].(float64),
			Percentile99: _r[3].(float64),
			NumCalls:     uint64(_r[4].(float64)),
		})
	}

	zap.S().Info(query)

	if err != nil {
		zap.S().Debug("Error in processing sql query: ", err)
		return nil, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query")}
	}

	serviceErrorItems := []model.ServiceErrorItem{}

	query = fmt.Sprintf("SELECT DATETIMECONVERT(\"timestamp\", '1:NANOSECONDS:EPOCH', '1:MINUTES:EPOCH', '%s:MINUTES') AS \"time\", count(*) as numErrors FROM %s WHERE \"timestamp\">=%s AND \"timestamp\"<=%s AND kind='2' AND serviceName='%s' AND hasError=true", strconv.Itoa(int(queryParams.StepSeconds/60)), r.indexTable, strconv.FormatInt(queryParams.Start.UnixNano(), 10), strconv.FormatInt(queryParams.End.UnixNano(), 10), queryParams.ServiceName)
	// args = []interface{}{}
	// args, errStatus = buildQueryWithTagParams(ctx, queryParams.Tags, &query, args)
	// if errStatus != nil {
	// 	return nil, errStatus
	// }
	query += " GROUP BY \"time\" ORDER BY \"time\" DESC LIMIT " + DEFAULT_UNLIMITED_LIMIT
	rows, err = r.db.ControllerQuery(ctx, query)

	for _, row := range rows.rows {
		_r := row.([]interface{})
		serviceErrorItems = append(serviceErrorItems, model.ServiceErrorItem{
			Time:      time.Unix(0, int64(_r[0].(float64))*int64(time.Minute)),
			NumErrors: uint64(_r[1].(float64)),
		})
	}

	zap.S().Info(query)

	if err != nil {
		zap.S().Debug("Error in processing sql query: ", err)
		return nil, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query")}
	}

	m := make(map[int64]int)

	for j := range serviceErrorItems {
		m[int64(serviceErrorItems[j].Time.UnixNano())] = int(serviceErrorItems[j].NumErrors)
	}

	for i := range serviceOverviewItems {
		serviceOverviewItems[i].Timestamp = int64(serviceOverviewItems[i].Time.UnixNano())

		if val, ok := m[serviceOverviewItems[i].Timestamp]; ok {
			serviceOverviewItems[i].NumErrors = uint64(val)
		}
		serviceOverviewItems[i].ErrorRate = float64(serviceOverviewItems[i].NumErrors) * 100 / float64(serviceOverviewItems[i].NumCalls)
		serviceOverviewItems[i].CallRate = float64(serviceOverviewItems[i].NumCalls) / float64(queryParams.StepSeconds)
	}

	return &serviceOverviewItems, nil
}

func buildFilterArrayQuery(ctx context.Context, excludeMap map[string]struct{}, params []string, filter string, query *string, args []interface{}) []interface{} {
	for i, e := range params {
		if i == 0 && i == len(params)-1 {
			if _, ok := excludeMap[filter]; ok {
				*query += fmt.Sprintf(" AND NOT (%s='%s')", filter, e)
			} else {
				*query += fmt.Sprintf(" AND (%s='%s')", filter, e)
			}
		} else if i == 0 && i != len(params)-1 {
			if _, ok := excludeMap[filter]; ok {
				*query += fmt.Sprintf(" AND NOT (%s='%s'", filter, e)
			} else {
				*query += fmt.Sprintf(" AND (%s='%s'", filter, e)
			}
		} else if i != 0 && i == len(params)-1 {
			*query += fmt.Sprintf(" OR %s='%s')", filter, e)
		} else {
			*query += fmt.Sprintf(" OR %s='%s'", filter, e)
		}
	}
	return args
}

func (r *PinotReader) GetSpanFilters(ctx context.Context, queryParams *model.SpanFilterParams) (*model.SpanFiltersResponse, *model.ApiError) {

	var query string
	excludeMap := make(map[string]struct{})
	for _, e := range queryParams.Exclude {
		if e == constants.OperationRequest {
			excludeMap[constants.OperationDB] = struct{}{}
			continue
		}
		excludeMap[e] = struct{}{}
	}

	startTimeNanos := strconv.FormatInt(queryParams.Start.UnixNano(), 10)
	endTimeNanos := strconv.FormatInt(queryParams.End.UnixNano(), 10)

	args := []interface{}{}
	if len(queryParams.ServiceName) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.ServiceName, constants.ServiceName, &query, args)
	}
	if len(queryParams.HttpRoute) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.HttpRoute, constants.HttpRoute, &query, args)
	}
	if len(queryParams.HttpCode) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.HttpCode, constants.HttpCode, &query, args)
	}
	if len(queryParams.HttpHost) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.HttpHost, constants.HttpHost, &query, args)
	}
	if len(queryParams.HttpMethod) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.HttpMethod, constants.HttpMethod, &query, args)
	}
	if len(queryParams.HttpUrl) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.HttpUrl, constants.HttpUrl, &query, args)
	}
	if len(queryParams.Component) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.Component, constants.Component, &query, args)
	}
	if len(queryParams.Operation) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.Operation, constants.OperationDB, &query, args)
	}
	if len(queryParams.RPCMethod) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.RPCMethod, constants.RPCMethod, &query, args)
	}
	if len(queryParams.ResponseStatusCode) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.ResponseStatusCode, constants.ResponseStatusCode, &query, args)
	}

	if len(queryParams.MinDuration) != 0 {
		query = query + " AND durationNano >= " + queryParams.MinDuration
	}
	if len(queryParams.MaxDuration) != 0 {
		query = query + " AND durationNano <= " + queryParams.MaxDuration
	}

	query = getStatusFilters(query, queryParams.Status, excludeMap)

	traceFilterReponse := model.SpanFiltersResponse{
		Status:             map[string]uint64{},
		Duration:           map[string]uint64{},
		ServiceName:        map[string]uint64{},
		Operation:          map[string]uint64{},
		ResponseStatusCode: map[string]uint64{},
		RPCMethod:          map[string]uint64{},
		HttpCode:           map[string]uint64{},
		HttpMethod:         map[string]uint64{},
		HttpUrl:            map[string]uint64{},
		HttpRoute:          map[string]uint64{},
		HttpHost:           map[string]uint64{},
		Component:          map[string]uint64{},
	}

	for _, e := range queryParams.GetFilters {
		switch e {
		case constants.ServiceName:
			finalQuery := fmt.Sprintf("SELECT serviceName, count(*) as count FROM %s WHERE \"timestamp\" >= %s AND \"timestamp\" <= %s", r.indexTable, startTimeNanos, endTimeNanos)
			finalQuery += query
			finalQuery += " GROUP BY serviceName LIMIT " + DEFAULT_UNLIMITED_LIMIT
			var dBResponse []model.DBResponseServiceName
			rows, err := r.db.ControllerQuery(ctx, finalQuery)
			zap.S().Info(finalQuery)

			for _, row := range rows.rows {
				_r := row.([]interface{})
				dBResponse = append(dBResponse, model.DBResponseServiceName{
					ServiceName: _r[0].(string),
					Count:       uint64(_r[1].(float64)),
				})
			}

			if err != nil {
				zap.S().Debug("Error in processing sql query: ", err)
				return nil, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query: %s", err)}
			}
			for _, service := range dBResponse {
				if service.ServiceName != "" {
					traceFilterReponse.ServiceName[service.ServiceName] = service.Count
				}
			}
		case constants.HttpCode:
			finalQuery := fmt.Sprintf("SELECT httpCode, count(*) as count FROM %s WHERE \"timestamp\" >= %s AND \"timestamp\" <= %s", r.indexTable, startTimeNanos, endTimeNanos)
			finalQuery += query
			finalQuery += " GROUP BY httpCode LIMIT " + DEFAULT_UNLIMITED_LIMIT
			var dBResponse []model.DBResponseHttpCode
			rows, err := r.db.ControllerQuery(ctx, finalQuery)
			zap.S().Info(finalQuery)

			for _, row := range rows.rows {
				_r := row.([]interface{})
				dBResponse = append(dBResponse, model.DBResponseHttpCode{
					HttpCode: _r[0].(string),
					Count:    uint64(_r[1].(float64)),
				})
			}

			if err != nil {
				zap.S().Debug("Error in processing sql query: ", err)
				return nil, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query: %s", err)}
			}
			for _, service := range dBResponse {
				if service.HttpCode != "" {
					traceFilterReponse.HttpCode[service.HttpCode] = service.Count
				}
			}
		case constants.HttpRoute:
			finalQuery := fmt.Sprintf("SELECT httpRoute, count(*) as count FROM %s WHERE \"timestamp\" >= %s AND \"timestamp\" <= %s", r.indexTable, startTimeNanos, endTimeNanos)
			finalQuery += query
			finalQuery += " GROUP BY httpRoute LIMIT " + DEFAULT_UNLIMITED_LIMIT
			var dBResponse []model.DBResponseHttpRoute
			rows, err := r.db.ControllerQuery(ctx, finalQuery)
			zap.S().Info(finalQuery)

			for _, row := range rows.rows {
				_r := row.([]interface{})
				dBResponse = append(dBResponse, model.DBResponseHttpRoute{
					HttpRoute: _r[0].(string),
					Count:     uint64(_r[1].(float64)),
				})
			}

			if err != nil {
				zap.S().Debug("Error in processing sql query: ", err)
				return nil, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query: %s", err)}
			}
			for _, service := range dBResponse {
				if service.HttpRoute != "" {
					traceFilterReponse.HttpRoute[service.HttpRoute] = service.Count
				}
			}
		case constants.HttpUrl:
			finalQuery := fmt.Sprintf("SELECT httpUrl, count(*) as count FROM %s WHERE \"timestamp\" >= %s AND \"timestamp\" <= %s", r.indexTable, startTimeNanos, endTimeNanos)
			finalQuery += query
			finalQuery += " GROUP BY httpUrl LIMIT " + DEFAULT_UNLIMITED_LIMIT
			var dBResponse []model.DBResponseHttpUrl
			rows, err := r.db.ControllerQuery(ctx, finalQuery)
			zap.S().Info(finalQuery)

			for _, row := range rows.rows {
				_r := row.([]interface{})
				dBResponse = append(dBResponse, model.DBResponseHttpUrl{
					HttpUrl: _r[0].(string),
					Count:   uint64(_r[1].(float64)),
				})
			}
			if err != nil {
				zap.S().Debug("Error in processing sql query: ", err)
				return nil, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query: %s", err)}
			}
			for _, service := range dBResponse {
				if service.HttpUrl != "" {
					traceFilterReponse.HttpUrl[service.HttpUrl] = service.Count
				}
			}
		case constants.HttpMethod:
			finalQuery := fmt.Sprintf("SELECT httpMethod, count(*) as count FROM %s WHERE \"timestamp\" >= %s AND \"timestamp\" <= %s", r.indexTable, startTimeNanos, endTimeNanos)
			finalQuery += query
			finalQuery += " GROUP BY httpMethod LIMIT " + DEFAULT_UNLIMITED_LIMIT
			var dBResponse []model.DBResponseHttpMethod
			rows, err := r.db.ControllerQuery(ctx, finalQuery)
			zap.S().Info(finalQuery)

			for _, row := range rows.rows {
				_r := row.([]interface{})
				dBResponse = append(dBResponse, model.DBResponseHttpMethod{
					HttpMethod: _r[0].(string),
					Count:      uint64(_r[1].(float64)),
				})
			}

			if err != nil {
				zap.S().Debug("Error in processing sql query: ", err)
				return nil, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query: %s", err)}
			}
			for _, service := range dBResponse {
				if service.HttpMethod != "" {
					traceFilterReponse.HttpMethod[service.HttpMethod] = service.Count
				}
			}
		case constants.HttpHost:
			finalQuery := fmt.Sprintf("SELECT httpHost, count(*) as count FROM %s WHERE \"timestamp\" >= %s AND \"timestamp\" <= %s", r.indexTable, startTimeNanos, endTimeNanos)
			finalQuery += query
			finalQuery += " GROUP BY httpHost LIMIT " + DEFAULT_UNLIMITED_LIMIT
			var dBResponse []model.DBResponseHttpHost
			rows, err := r.db.ControllerQuery(ctx, finalQuery)
			zap.S().Info(finalQuery)

			for _, row := range rows.rows {
				_r := row.([]interface{})
				dBResponse = append(dBResponse, model.DBResponseHttpHost{
					HttpHost: _r[0].(string),
					Count:    uint64(_r[1].(float64)),
				})
			}

			if err != nil {
				zap.S().Debug("Error in processing sql query: ", err)
				return nil, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query: %s", err)}
			}
			for _, service := range dBResponse {
				if service.HttpHost != "" {
					traceFilterReponse.HttpHost[service.HttpHost] = service.Count
				}
			}
		case constants.OperationRequest:
			finalQuery := fmt.Sprintf("SELECT name, count(*) as count FROM %s WHERE \"timestamp\" >= %s AND \"timestamp\" <= %s", r.indexTable, startTimeNanos, endTimeNanos)
			finalQuery += query
			finalQuery += " GROUP BY name LIMIT " + DEFAULT_UNLIMITED_LIMIT
			var dBResponse []model.DBResponseOperation
			rows, err := r.db.ControllerQuery(ctx, finalQuery)
			zap.S().Info(finalQuery)

			for _, row := range rows.rows {
				_r := row.([]interface{})
				dBResponse = append(dBResponse, model.DBResponseOperation{
					Operation: _r[0].(string),
					Count:     uint64(_r[1].(float64)),
				})
			}

			if err != nil {
				zap.S().Debug("Error in processing sql query: ", err)
				return nil, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query: %s", err)}
			}
			for _, service := range dBResponse {
				if service.Operation != "" {
					traceFilterReponse.Operation[service.Operation] = service.Count
				}
			}
		case constants.Component:
			finalQuery := fmt.Sprintf("SELECT component, count(*) as count FROM %s WHERE \"timestamp\" >= %s AND \"timestamp\" <= %s", r.indexTable, startTimeNanos, endTimeNanos)
			finalQuery += query
			finalQuery += " GROUP BY component LIMIT " + DEFAULT_UNLIMITED_LIMIT
			var dBResponse []model.DBResponseComponent
			rows, err := r.db.ControllerQuery(ctx, finalQuery)
			zap.S().Info(finalQuery)

			for _, row := range rows.rows {
				_r := row.([]interface{})
				dBResponse = append(dBResponse, model.DBResponseComponent{
					Component: _r[0].(string),
					Count:     uint64(_r[1].(float64)),
				})
			}

			if err != nil {
				zap.S().Debug("Error in processing sql query: ", err)
				return nil, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query: %s", err)}
			}
			for _, service := range dBResponse {
				if service.Component != "" {
					traceFilterReponse.Component[service.Component] = service.Count
				}
			}
		case constants.Status:
			finalQuery := fmt.Sprintf("SELECT COUNT(*) as numTotal FROM %s WHERE \"timestamp\" >= %s AND \"timestamp\" <= %s AND hasError = true", r.indexTable, startTimeNanos, endTimeNanos)
			finalQuery += query
			finalQuery += " LIMIT " + DEFAULT_UNLIMITED_LIMIT
			var dBResponse []model.DBResponseTotal
			rows, err := r.db.ControllerQuery(ctx, finalQuery)
			zap.S().Info(finalQuery)

			for _, row := range rows.rows {
				_r := row.([]interface{})
				dBResponse = append(dBResponse, model.DBResponseTotal{
					NumTotal: uint64(_r[0].(float64)),
				})
			}

			if err != nil {
				zap.S().Debug("Error in processing sql query: ", err)
				return nil, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query: %s", err)}
			}

			finalQuery2 := fmt.Sprintf("SELECT COUNT(*) as numTotal FROM %s WHERE \"timestamp\" >= %s AND \"timestamp\" <= %s AND hasError = false ", r.indexTable, startTimeNanos, endTimeNanos)
			finalQuery2 += query
			finalQuery2 += " LIMIT " + DEFAULT_UNLIMITED_LIMIT
			var dBResponse2 []model.DBResponseTotal
			rows, err = r.db.ControllerQuery(ctx, finalQuery2)
			zap.S().Info(finalQuery2)

			for _, row := range rows.rows {
				_r := row.([]interface{})
				dBResponse2 = append(dBResponse2, model.DBResponseTotal{
					NumTotal: uint64(_r[0].(float64)),
				})
			}

			if err != nil {
				zap.S().Debug("Error in processing sql query: ", err)
				return nil, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query: %s", err)}
			}
			if len(dBResponse) > 0 && len(dBResponse2) > 0 {
				traceFilterReponse.Status = map[string]uint64{"ok": dBResponse2[0].NumTotal, "error": dBResponse[0].NumTotal}
			} else if len(dBResponse) > 0 {
				traceFilterReponse.Status = map[string]uint64{"ok": 0, "error": dBResponse[0].NumTotal}
			} else if len(dBResponse2) > 0 {
				traceFilterReponse.Status = map[string]uint64{"ok": dBResponse2[0].NumTotal, "error": 0}
			} else {
				traceFilterReponse.Status = map[string]uint64{"ok": 0, "error": 0}
			}
		case constants.Duration:
			finalQuery := fmt.Sprintf("SELECT durationNano as numTotal FROM %s WHERE \"timestamp\" >= %s AND \"timestamp\" <= %s", r.indexTable, startTimeNanos, endTimeNanos)
			finalQuery += query
			finalQuery += " ORDER BY durationNano LIMIT 1"
			var dBResponse []model.DBResponseTotal
			rows, err := r.db.ControllerQuery(ctx, finalQuery)
			zap.S().Info(finalQuery)

			for _, row := range rows.rows {
				_r := row.([]interface{})
				dBResponse = append(dBResponse, model.DBResponseTotal{
					NumTotal: uint64(_r[0].(float64)),
				})
			}

			if err != nil {
				zap.S().Debug("Error in processing sql query: ", err)
				return nil, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query: %s", err)}
			}
			finalQuery = fmt.Sprintf("SELECT durationNano as numTotal FROM %s WHERE \"timestamp\" >= %s AND \"timestamp\" <= %s", r.indexTable, startTimeNanos, endTimeNanos)
			finalQuery += query
			finalQuery += " ORDER BY durationNano DESC LIMIT 1"
			var dBResponse2 []model.DBResponseTotal
			rows, err = r.db.ControllerQuery(ctx, finalQuery)
			zap.S().Info(finalQuery)

			for _, row := range rows.rows {
				_r := row.([]interface{})
				dBResponse2 = append(dBResponse2, model.DBResponseTotal{
					NumTotal: uint64(_r[0].(float64)),
				})
			}

			if err != nil {
				zap.S().Debug("Error in processing sql query: ", err)
				return nil, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query: %s", err)}
			}
			if len(dBResponse) > 0 {
				traceFilterReponse.Duration["minDuration"] = dBResponse[0].NumTotal
			}
			if len(dBResponse2) > 0 {
				traceFilterReponse.Duration["maxDuration"] = dBResponse2[0].NumTotal
			}
		case constants.RPCMethod:
			finalQuery := fmt.Sprintf("SELECT rpcMethod, count(*) as count FROM %s WHERE \"timestamp\" >= %s AND \"timestamp\" <= %s", r.indexTable, startTimeNanos, endTimeNanos)
			finalQuery += query
			finalQuery += " GROUP BY rpcMethod LIMIT " + DEFAULT_UNLIMITED_LIMIT
			var dBResponse []model.DBResponseRPCMethod
			rows, err := r.db.ControllerQuery(ctx, finalQuery)
			zap.S().Info(finalQuery)

			for _, row := range rows.rows {
				_r := row.([]interface{})
				dBResponse = append(dBResponse, model.DBResponseRPCMethod{
					RPCMethod: _r[0].(string),
					Count:     uint64(_r[1].(float64)),
				})
			}

			if err != nil {
				zap.S().Debug("Error in processing sql query: ", err)
				return nil, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("error in processing sql query: %s", err)}
			}
			for _, service := range dBResponse {
				if service.RPCMethod != "" {
					traceFilterReponse.RPCMethod[service.RPCMethod] = service.Count
				}
			}

		case constants.ResponseStatusCode:
			finalQuery := fmt.Sprintf("SELECT responseStatusCode, count(*) as count FROM %s WHERE \"timestamp\" >= %s AND \"timestamp\" <= %s", r.indexTable, startTimeNanos, endTimeNanos)
			finalQuery += query
			finalQuery += " GROUP BY responseStatusCode LIMIT " + DEFAULT_UNLIMITED_LIMIT
			var dBResponse []model.DBResponseStatusCodeMethod
			rows, err := r.db.ControllerQuery(ctx, finalQuery)
			zap.S().Info(finalQuery)

			for _, row := range rows.rows {
				_r := row.([]interface{})
				dBResponse = append(dBResponse, model.DBResponseStatusCodeMethod{
					ResponseStatusCode: _r[0].(string),
					Count:              uint64(_r[1].(float64)),
				})
			}

			if err != nil {
				zap.S().Debug("Error in processing sql query: ", err)
				return nil, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("error in processing sql query: %s", err)}
			}
			for _, service := range dBResponse {
				if service.ResponseStatusCode != "" {
					traceFilterReponse.ResponseStatusCode[service.ResponseStatusCode] = service.Count
				}
			}

		default:
			return nil, &model.ApiError{Typ: model.ErrorBadData, Err: fmt.Errorf("filter type: %s not supported", e)}
		}
	}

	return &traceFilterReponse, nil
}

func getStatusFilters(query string, statusParams []string, excludeMap map[string]struct{}) string {

	// status can only be two and if both are selected than they are equivalent to none selected
	if _, ok := excludeMap["status"]; ok {
		if len(statusParams) == 1 {
			if statusParams[0] == "error" {
				query += " AND hasError = false"
			} else if statusParams[0] == "ok" {
				query += " AND hasError = true"
			}
		}
	} else if len(statusParams) == 1 {
		if statusParams[0] == "error" {
			query += " AND hasError = true"
		} else if statusParams[0] == "ok" {
			query += " AND hasError = false"
		}
	}
	return query
}

func (r *PinotReader) GetFilteredSpans(ctx context.Context, queryParams *model.GetFilteredSpansParams) (*model.GetFilterSpansResponse, *model.ApiError) {

	queryTable := fmt.Sprintf("%s", r.indexTable)

	excludeMap := make(map[string]struct{})
	for _, e := range queryParams.Exclude {
		if e == constants.OperationRequest {
			excludeMap[constants.OperationDB] = struct{}{}
			continue
		}
		excludeMap[e] = struct{}{}
	}

	startTimeNanos := strconv.FormatInt(queryParams.Start.UnixNano(), 10)
	endTimeNanos := strconv.FormatInt(queryParams.End.UnixNano(), 10)

	var query string
	args := []interface{}{}
	if len(queryParams.ServiceName) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.ServiceName, constants.ServiceName, &query, args)
	}
	if len(queryParams.HttpRoute) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.HttpRoute, constants.HttpRoute, &query, args)
	}
	if len(queryParams.HttpCode) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.HttpCode, constants.HttpCode, &query, args)
	}
	if len(queryParams.HttpHost) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.HttpHost, constants.HttpHost, &query, args)
	}
	if len(queryParams.HttpMethod) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.HttpMethod, constants.HttpMethod, &query, args)
	}
	if len(queryParams.HttpUrl) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.HttpUrl, constants.HttpUrl, &query, args)
	}
	if len(queryParams.Component) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.Component, constants.Component, &query, args)
	}
	if len(queryParams.Operation) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.Operation, constants.OperationDB, &query, args)
	}
	if len(queryParams.RPCMethod) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.RPCMethod, constants.RPCMethod, &query, args)
	}

	if len(queryParams.ResponseStatusCode) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.ResponseStatusCode, constants.ResponseStatusCode, &query, args)
	}

	if len(queryParams.MinDuration) != 0 {
		query = query + " AND durationNano >= " + queryParams.MinDuration
	}
	if len(queryParams.MaxDuration) != 0 {
		query = query + " AND durationNano <= " + queryParams.MaxDuration
	}
	query = getStatusFilters(query, queryParams.Status, excludeMap)

	if len(queryParams.Kind) != 0 {
		query = query + " AND kind = " + queryParams.Kind
	}

	// args, errStatus := buildQueryWithTagParams(ctx, queryParams.Tags, &query, args)
	// if errStatus != nil {
	// 	return nil, errStatus
	// }

	if len(queryParams.OrderParam) != 0 {
		if queryParams.OrderParam == constants.Duration {
			if queryParams.Order == constants.Descending {
				query = query + " ORDER BY durationNano DESC"
			}
			if queryParams.Order == constants.Ascending {
				query = query + " ORDER BY durationNano ASC"
			}
		} else if queryParams.OrderParam == constants.Timestamp {
			if queryParams.Order == constants.Descending {
				query = query + " ORDER BY \"timestamp\" DESC"
			}
			if queryParams.Order == constants.Ascending {
				query = query + " ORDER BY \"timestamp\" ASC"
			}
		}
	}
	if queryParams.Limit > 0 {
		if queryParams.Offset > 0 {
			query = query + " LIMIT " + strconv.FormatInt(queryParams.Offset, 10) + ", " + strconv.FormatInt(queryParams.Limit, 10)
		} else {
			query = query + " LIMIT " + strconv.FormatInt(queryParams.Limit, 10)
		}
	} else {
		query = query + " LIMIT " + DEFAULT_UNLIMITED_LIMIT
	}

	var getFilterSpansResponseItems []model.GetFilterSpansResponseItem

	baseQuery := fmt.Sprintf("SELECT \"timestamp\", spanID, traceID, serviceName, name, durationNano, httpCode, gRPCCode, gRPCMethod, httpMethod, rpcMethod, responseStatusCode FROM %s WHERE \"timestamp\" >= %s AND \"timestamp\" <= %s", queryTable, startTimeNanos, endTimeNanos)
	baseQuery += query

	rows, err := r.db.ControllerQuery(ctx, baseQuery)

	if err != nil {
		//
	}

	for _, row := range rows.rows {
		_r := row.([]interface{})

		if err != nil {
			//
		}

		getFilterSpansResponseItems = append(getFilterSpansResponseItems, model.GetFilterSpansResponseItem{
			Timestamp:          time.Unix(0, int64(_r[0].(float64))),
			SpanID:             _r[1].(string),
			TraceID:            _r[2].(string),
			ServiceName:        _r[3].(string),
			Operation:          _r[4].(string),
			DurationNano:       uint64(_r[5].(float64)),
			HttpCode:           _r[6].(string),
			GRPCode:            _r[7].(string),
			GRPMethod:          _r[8].(string),
			HttpMethod:         _r[9].(string),
			RPCMethod:          _r[10].(string),
			ResponseStatusCode: _r[11].(string),
		})
	}
	// Fill status and method
	for i, e := range getFilterSpansResponseItems {
		if e.GRPCode != "" {
			getFilterSpansResponseItems[i].StatusCode = e.GRPCode
		} else {
			getFilterSpansResponseItems[i].StatusCode = e.HttpCode
		}
		if e.GRPMethod != "" {
			getFilterSpansResponseItems[i].Method = e.GRPMethod
		} else {
			getFilterSpansResponseItems[i].Method = e.HttpMethod
		}
	}

	zap.S().Info(baseQuery)

	if err != nil {
		zap.S().Debug("Error in processing sql query: ", err)
		return nil, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query")}
	}

	getFilterSpansResponse := model.GetFilterSpansResponse{
		Spans:      getFilterSpansResponseItems,
		TotalSpans: 1000,
	}

	return &getFilterSpansResponse, nil
}

func StringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func String(length int) string {
	return StringWithCharset(length, charset)
}

func buildQueryWithTagParams(ctx context.Context, tags []model.TagQuery, query *string, args []interface{}) ([]interface{}, *model.ApiError) {

	for _, item := range tags {
		if item.Operator == "in" {
			for i, value := range item.Values {
				tagKey := "inTagKey" + String(5)
				tagValue := "inTagValue" + String(5)
				if i == 0 && i == len(item.Values)-1 {
					*query += fmt.Sprintf(" AND tagMap[@%s] = @%s", tagKey, tagValue)
				} else if i == 0 && i != len(item.Values)-1 {
					*query += fmt.Sprintf(" AND (tagMap[@%s] = @%s", tagKey, tagValue)
				} else if i != 0 && i == len(item.Values)-1 {
					*query += fmt.Sprintf(" OR tagMap[@%s] = @%s)", tagKey, tagValue)
				} else {
					*query += fmt.Sprintf(" OR tagMap[@%s] = @%s", tagKey, tagValue)
				}
				args = append(args, clickhouse.Named(tagKey, item.Key))
				args = append(args, clickhouse.Named(tagValue, value))
			}
		} else if item.Operator == "not in" {
			for i, value := range item.Values {
				tagKey := "notinTagKey" + String(5)
				tagValue := "notinTagValue" + String(5)
				if i == 0 && i == len(item.Values)-1 {
					*query += fmt.Sprintf(" AND NOT tagMap[@%s] = @%s", tagKey, tagValue)
				} else if i == 0 && i != len(item.Values)-1 {
					*query += fmt.Sprintf(" AND NOT (tagMap[@%s] = @%s", tagKey, tagValue)
				} else if i != 0 && i == len(item.Values)-1 {
					*query += fmt.Sprintf(" OR tagMap[@%s] = @%s)", tagKey, tagValue)
				} else {
					*query += fmt.Sprintf(" OR tagMap[@%s] = @%s", tagKey, tagValue)
				}
				args = append(args, clickhouse.Named(tagKey, item.Key))
				args = append(args, clickhouse.Named(tagValue, value))
			}
		} else {
			return nil, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Tag Operator %s not supported", item.Operator)}
		}
	}
	return args, nil
}

func (r *PinotReader) GetTagFilters(ctx context.Context, queryParams *model.TagFilterParams) (*[]model.TagFilters, *model.ApiError) {

	startTimeNanos := strconv.FormatInt(queryParams.Start.UnixNano(), 10)
	endTimeNanos := strconv.FormatInt(queryParams.End.UnixNano(), 10)

	excludeMap := make(map[string]struct{})
	for _, e := range queryParams.Exclude {
		if e == constants.OperationRequest {
			excludeMap[constants.OperationDB] = struct{}{}
			continue
		}
		excludeMap[e] = struct{}{}
	}

	var query string
	args := []interface{}{}
	if len(queryParams.ServiceName) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.ServiceName, constants.ServiceName, &query, args)
	}
	if len(queryParams.HttpRoute) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.HttpRoute, constants.HttpRoute, &query, args)
	}
	if len(queryParams.HttpCode) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.HttpCode, constants.HttpCode, &query, args)
	}
	if len(queryParams.HttpHost) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.HttpHost, constants.HttpHost, &query, args)
	}
	if len(queryParams.HttpMethod) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.HttpMethod, constants.HttpMethod, &query, args)
	}
	if len(queryParams.HttpUrl) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.HttpUrl, constants.HttpUrl, &query, args)
	}
	if len(queryParams.Component) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.Component, constants.Component, &query, args)
	}
	if len(queryParams.Operation) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.Operation, constants.OperationDB, &query, args)
	}
	if len(queryParams.RPCMethod) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.RPCMethod, constants.RPCMethod, &query, args)
	}
	if len(queryParams.ResponseStatusCode) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.ResponseStatusCode, constants.ResponseStatusCode, &query, args)
	}
	if len(queryParams.MinDuration) != 0 {
		query = query + " AND durationNano >= " + queryParams.MinDuration
	}
	if len(queryParams.MaxDuration) != 0 {
		query = query + " AND durationNano <= " + queryParams.MaxDuration
	}

	query = getStatusFilters(query, queryParams.Status, excludeMap)
	query = query + " LIMIT " + DEFAULT_UNLIMITED_LIMIT

	tagFilters := []model.TagFilters{}

	finalQuery := fmt.Sprintf(`select JSONEXTRACTKEY(tagMap, '$.*') as tagKeys FROM %s WHERE "timestamp" >= %s AND "timestamp" <= %s`, r.indexTable, startTimeNanos, endTimeNanos)
	// Alternative query: SELECT groupUniqArrayArray(mapKeys(tagMap)) as tagKeys  FROM signoz_index_v2
	finalQuery += query
	rows, err := r.db.ControllerQuery(ctx, finalQuery)

	for _, row := range rows.rows {
		_r := row.([]interface{})
		keys := _r[0].([]interface{})

		for _, k := range keys {
			tagFilters = append(tagFilters, model.TagFilters{
				TagKeys: k.(string),
			})
		}
	}

	zap.S().Info(query)

	if err != nil {
		zap.S().Debug("Error in processing sql query: ", err)
		return nil, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query")}
	}
	tagFilters = excludeTags(ctx, tagFilters)

	return &tagFilters, nil
}

func excludeTags(ctx context.Context, tags []model.TagFilters) []model.TagFilters {
	excludedTagsMap := map[string]bool{
		"http.code":           true,
		"http.route":          true,
		"http.method":         true,
		"http.url":            true,
		"http.status_code":    true,
		"http.host":           true,
		"messaging.system":    true,
		"messaging.operation": true,
		"component":           true,
		"error":               true,
		"service.name":        true,
	}
	var newTags []model.TagFilters
	for _, tag := range tags {
		_, ok := excludedTagsMap[tag.TagKeys]
		if !ok {
			newTags = append(newTags, tag)
		}
	}
	return newTags
}

func (r *PinotReader) GetTagValues(ctx context.Context, queryParams *model.TagFilterParams) (*[]model.TagValues, *model.ApiError) {
	return nil, &model.ApiError{model.ErrorNotImplemented, fmt.Errorf("Pinot does not support operation")}
}

func (r *PinotReader) GetTopEndpoints(ctx context.Context, queryParams *model.GetTopEndpointsParams) (*[]model.TopEndpointsItem, *model.ApiError) {

	var topEndpointsItems []model.TopEndpointsItem

	query := fmt.Sprintf("SELECT percentileest(durationNano, 50) as p50, percentileest(durationNano, 95) as p95, percentileest(durationNano, 99) as p99, COUNT(*) as numCalls, name  FROM %s WHERE  \"timestamp\" >= %s AND \"timestamp\" <= %s AND  kind='2' and serviceName='%s'", r.indexTable, strconv.FormatInt(queryParams.Start.UnixNano(), 10), strconv.FormatInt(queryParams.End.UnixNano(), 10), queryParams.ServiceName)
	// args := []interface{}{}
	// args, errStatus := buildQueryWithTagParams(ctx, queryParams.Tags, &query, args)
	// if errStatus != nil {
	// 	return nil, errStatus
	// }
	query += " GROUP BY name LIMIT " + DEFAULT_UNLIMITED_LIMIT
	rows, err := r.db.ControllerQuery(ctx, query)

	for _, row := range rows.rows {
		_r := row.([]interface{})

		topEndpointsItems = append(topEndpointsItems, model.TopEndpointsItem{
			Percentile50: _r[0].(float64),
			Percentile95: _r[1].(float64),
			Percentile99: _r[2].(float64),
			NumCalls:     uint64(_r[3].(float64)),
			Name:         _r[4].(string),
		})
	}

	zap.S().Info(query)

	if err != nil {
		zap.S().Debug("Error in processing sql query: ", err)
		return nil, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query")}
	}

	if topEndpointsItems == nil {
		topEndpointsItems = []model.TopEndpointsItem{}
	}

	return &topEndpointsItems, nil
}

func (r *PinotReader) GetUsage(ctx context.Context, queryParams *model.GetUsageParams) (*[]model.UsageItem, error) {

	var usageItems []model.UsageItem

	var query string
	if len(queryParams.ServiceName) != 0 {
		query = fmt.Sprintf("SELECT TIMECONVERT(\"timestamp\", 'NANOSECONDS', 'MINUTES') as \"time\", count(*) as count FROM %s WHERE serviceName='%s' AND \"timestamp\">=%s AND \"timestamp\"<=%s GROUP BY \"time\" ORDER BY \"time\" ASC LIMIT %s", r.indexTable, queryParams.ServiceName, strconv.FormatInt(queryParams.Start.UnixNano(), 10), strconv.FormatInt(queryParams.End.UnixNano(), 10), DEFAULT_UNLIMITED_LIMIT)
	} else {
		query = fmt.Sprintf("SELECT TIMECONVERT(\"timestamp\", 'NANOSECONDS', 'MINUTES') as \"time\", count(1) as count FROM %s WHERE \"timestamp\">=%s AND \"timestamp\"<=%s GROUP BY \"time\" ORDER BY \"time\" ASC LIMIT %s", r.indexTable, strconv.FormatInt(queryParams.Start.UnixNano(), 10), strconv.FormatInt(queryParams.End.UnixNano(), 10), DEFAULT_UNLIMITED_LIMIT)
	}

	rows, err := r.db.ControllerQuery(ctx, query)

	for _, row := range rows.rows {
		_r := row.([]interface{})

		timeMs, err := strconv.ParseInt(_r[0].(string), 10, 64)
		if err != nil {
			//
		}
		time := time.Unix(0, timeMs*int64(time.Minute))

		usageItems = append(usageItems, model.UsageItem{
			Time:  time,
			Count: _r[1].(uint64),
		})
	}

	zap.S().Info(query)

	if err != nil {
		zap.S().Debug("Error in processing sql query: ", err)
		return nil, fmt.Errorf("Error in processing sql query")
	}

	for i := range usageItems {
		usageItems[i].Timestamp = uint64(usageItems[i].Time.UnixNano())
	}

	if usageItems == nil {
		usageItems = []model.UsageItem{}
	}

	return &usageItems, nil
}

func (r *PinotReader) SearchTraces(ctx context.Context, traceId string) (*[]model.SearchSpansResult, error) {

	var searchScanReponses []model.SearchSpanDBReponseItem

	query := fmt.Sprintf("SELECT \"timestamp\", traceID, model FROM %s WHERE traceID='%s' LIMIT %s", r.spansTable, traceId, DEFAULT_UNLIMITED_LIMIT)

	rows, err := r.db.ControllerQuery(ctx, query)

	for _, row := range rows.rows {
		_r := row.([]interface{})

		searchScanReponses = append(searchScanReponses, model.SearchSpanDBReponseItem{
			Timestamp: time.Unix(0, int64(_r[0].(float64))),
			TraceID:   _r[1].(string),
			Model:     _r[2].(string),
		})
	}

	zap.S().Info(query)

	if err != nil {
		zap.S().Debug("Error in processing sql query: ", err)
		return nil, fmt.Errorf("Error in processing sql query")
	}

	searchSpansResult := []model.SearchSpansResult{{
		Columns: []string{"__time", "SpanId", "TraceId", "ServiceName", "Name", "Kind", "DurationNano", "TagsKeys", "TagsValues", "References", "Events", "HasError"},
		Events:  make([][]interface{}, len(searchScanReponses)),
	},
	}

	for i, item := range searchScanReponses {
		var jsonItem model.SearchSpanReponseItem
		json.Unmarshal([]byte(item.Model), &jsonItem)
		jsonItem.TimeUnixNano = uint64(item.Timestamp.UnixNano() / 1000000)
		spanEvents := jsonItem.GetValues()
		searchSpansResult[0].Events[i] = spanEvents
	}

	return &searchSpansResult, nil

}
func interfaceArrayToStringArray(array []interface{}) []string {
	var strArray []string
	for _, item := range array {
		strArray = append(strArray, item.(string))
	}
	return strArray
}

func (r *PinotReader) GetServiceMapDependencies(ctx context.Context, queryParams *model.GetServicesParams) (*[]model.ServiceMapDependencyResponseItem, error) {
	serviceMapDependencyItems := []model.ServiceMapDependencyItem{}

	query := fmt.Sprintf(`SELECT spanID, parentSpanID, serviceName FROM %s WHERE "timestamp">=%s AND "timestamp"<=%s LIMIT %s`, r.indexTable, strconv.FormatInt(queryParams.Start.UnixNano(), 10), strconv.FormatInt(queryParams.End.UnixNano(), 10), DEFAULT_UNLIMITED_LIMIT)

	rows, err := r.db.ControllerQuery(ctx, query)

	for _, row := range rows.rows {
		_r := row.([]interface{})

		serviceMapDependencyItems = append(serviceMapDependencyItems, model.ServiceMapDependencyItem{
			SpanId:       _r[0].(string),
			ParentSpanId: _r[1].(string),
			ServiceName:  _r[2].(string),
		})
	}

	zap.S().Info(query)

	if err != nil {
		zap.S().Debug("Error in processing sql query: ", err)
		return nil, fmt.Errorf("Error in processing sql query")
	}

	serviceMap := make(map[string]*model.ServiceMapDependencyResponseItem)

	spanId2ServiceNameMap := make(map[string]string)
	for i := range serviceMapDependencyItems {
		spanId2ServiceNameMap[serviceMapDependencyItems[i].SpanId] = serviceMapDependencyItems[i].ServiceName
	}
	for i := range serviceMapDependencyItems {
		parent2childServiceName := spanId2ServiceNameMap[serviceMapDependencyItems[i].ParentSpanId] + "-" + spanId2ServiceNameMap[serviceMapDependencyItems[i].SpanId]
		if _, ok := serviceMap[parent2childServiceName]; !ok {
			serviceMap[parent2childServiceName] = &model.ServiceMapDependencyResponseItem{
				Parent:    spanId2ServiceNameMap[serviceMapDependencyItems[i].ParentSpanId],
				Child:     spanId2ServiceNameMap[serviceMapDependencyItems[i].SpanId],
				CallCount: 1,
			}
		} else {
			serviceMap[parent2childServiceName].CallCount++
		}
	}

	retMe := make([]model.ServiceMapDependencyResponseItem, 0, len(serviceMap))
	for _, dependency := range serviceMap {
		if dependency.Parent == "" {
			continue
		}
		retMe = append(retMe, *dependency)
	}

	return &retMe, nil
}

func (r *PinotReader) GetFilteredSpansAggregates(ctx context.Context, queryParams *model.GetFilteredSpanAggregatesParams) (*model.GetFilteredSpansAggregatesResponse, *model.ApiError) {

	excludeMap := make(map[string]struct{})
	for _, e := range queryParams.Exclude {
		if e == constants.OperationRequest {
			excludeMap[constants.OperationDB] = struct{}{}
			continue
		}
		excludeMap[e] = struct{}{}
	}

	SpanAggregatesDBResponseItems := []model.SpanAggregatesDBResponseItem{}

	aggregation_query := ""
	if queryParams.Dimension == "duration" {
		switch queryParams.AggregationOption {
		case "p50":
			aggregation_query = " percentileest(durationNano, 50) as float64Value "
		case "p95":
			aggregation_query = " percentileest(durationNano, 95) as float64Value "
		case "p90":
			aggregation_query = " percentileest(durationNano, 90) as float64Value "
		case "p99":
			aggregation_query = " percentileest(durationNano, 99) as float64Value "
		case "max":
			aggregation_query = " max(durationNano) as value "
		case "min":
			aggregation_query = " min(durationNano) as value "
		case "avg":
			aggregation_query = " avg(durationNano) as float64Value "
		case "sum":
			aggregation_query = " sum(durationNano) as value "
		default:
			return nil, &model.ApiError{Typ: model.ErrorBadData, Err: fmt.Errorf("Aggregate type: %s not supported", queryParams.AggregationOption)}
		}
	} else if queryParams.Dimension == "calls" {
		aggregation_query = " count(*) as value "
	}

	startTimeNanos := strconv.FormatInt(queryParams.Start.UnixNano(), 10)
	endTimeNanos := strconv.FormatInt(queryParams.End.UnixNano(), 10)
	args := []interface{}{}

	var query string
	if queryParams.GroupBy != "" {
		switch queryParams.GroupBy {
		case constants.ServiceName:
			query = fmt.Sprintf("SELECT TIMECONVERT(\"timestamp\", 'NANOSECONDS', 'MINUTES') AS \"time\", serviceName as groupBy, %s FROM %s WHERE \"timestamp\" >= %s AND \"timestamp\" <= %s", aggregation_query, r.indexTable, startTimeNanos, endTimeNanos)
		case constants.HttpCode:
			query = fmt.Sprintf("SELECT TIMECONVERT(\"timestamp\", 'NANOSECONDS', 'MINUTES') AS \"time\", httpCode as groupBy, %s FROM %s WHERE \"timestamp\" >= %s  AND \"timestamp\" <= %s", aggregation_query, r.indexTable, startTimeNanos, endTimeNanos)
		case constants.HttpMethod:
			query = fmt.Sprintf("SELECT TIMECONVERT(\"timestamp\", 'NANOSECONDS', 'MINUTES') AS \"time\", httpMethod as groupBy, %s FROM %s WHERE \"timestamp\" >= %s  AND \"timestamp\" <= %s", aggregation_query, r.indexTable, startTimeNanos, endTimeNanos)
		case constants.HttpUrl:
			query = fmt.Sprintf("SELECT TIMECONVERT(\"timestamp\", 'NANOSECONDS', 'MINUTES') AS \"time\", httpUrl as groupBy, %s FROM %s WHERE \"timestamp\" >= %s  AND \"timestamp\" <= %s", aggregation_query, r.indexTable, startTimeNanos, endTimeNanos)
		case constants.HttpRoute:
			query = fmt.Sprintf("SELECT TIMECONVERT(\"timestamp\", 'NANOSECONDS', 'MINUTES') AS \"time\", httpRoute as groupBy, %s FROM %s WHERE \"timestamp\" >= %s  AND \"timestamp\" <= %s", aggregation_query, r.indexTable, startTimeNanos, endTimeNanos)
		case constants.HttpHost:
			query = fmt.Sprintf("SELECT TIMECONVERT(\"timestamp\", 'NANOSECONDS', 'MINUTES') AS \"time\", httpHost as groupBy, %s FROM %s WHERE \"timestamp\" >= %s  AND \"timestamp\" <= %s", aggregation_query, r.indexTable, startTimeNanos, endTimeNanos)
		case constants.DBName:
			query = fmt.Sprintf("SELECT TIMECONVERT(\"timestamp\", 'NANOSECONDS', 'MINUTES') AS \"time\", dbName as groupBy, %s FROM %s WHERE \"timestamp\" >= %s  AND \"timestamp\" <= %s", aggregation_query, r.indexTable, startTimeNanos, endTimeNanos)
		case constants.DBOperation:
			query = fmt.Sprintf("SELECT TIMECONVERT(\"timestamp\", 'NANOSECONDS', 'MINUTES') AS \"time\", dbOperation as groupBy, %s FROM %s WHERE \"timestamp\" >= %s  AND \"timestamp\" <= %s", aggregation_query, r.indexTable, startTimeNanos, endTimeNanos)
		case constants.OperationRequest:
			query = fmt.Sprintf("SELECT TIMECONVERT(\"timestamp\", 'NANOSECONDS', 'MINUTES') AS \"time\", name as groupBy, %s FROM %s WHERE \"timestamp\" >= %s  AND \"timestamp\" <= %s", aggregation_query, r.indexTable, startTimeNanos, endTimeNanos)
		case constants.MsgSystem:
			query = fmt.Sprintf("SELECT TIMECONVERT(\"timestamp\", 'NANOSECONDS', 'MINUTES') AS \"time\", msgSystem as groupBy, %s FROM %s WHERE \"timestamp\" >= %s  AND \"timestamp\" <= %s", aggregation_query, r.indexTable, startTimeNanos, endTimeNanos)
		case constants.MsgOperation:
			query = fmt.Sprintf("SELECT TIMECONVERT(\"timestamp\", 'NANOSECONDS', 'MINUTES') AS \"time\", msgOperation as groupBy, %s FROM %s WHERE \"timestamp\" >= %s  AND \"timestamp\" <= %s", aggregation_query, r.indexTable, startTimeNanos, endTimeNanos)
		case constants.DBSystem:
			query = fmt.Sprintf("SELECT TIMECONVERT(\"timestamp\", 'NANOSECONDS', 'MINUTES') AS \"time\", dbSystem as groupBy, %s FROM %s WHERE \"timestamp\" >= %s  AND \"timestamp\" <= %s", aggregation_query, r.indexTable, startTimeNanos, endTimeNanos)
		case constants.Component:
			query = fmt.Sprintf("SELECT TIMECONVERT(\"timestamp\", 'NANOSECONDS', 'MINUTES') AS \"time\", component as groupBy, %s FROM %s WHERE \"timestamp\" >= %s  AND \"timestamp\" <= %s", aggregation_query, r.indexTable, startTimeNanos, endTimeNanos)
		case constants.RPCMethod:
			query = fmt.Sprintf("SELECT TIMECONVERT(\"timestamp\", 'NANOSECONDS', 'MINUTES') AS \"time\", rpcMethod as groupBy, %s FROM %s WHERE \"timestamp\" >= %s  AND \"timestamp\" <= %s", aggregation_query, r.indexTable, startTimeNanos, endTimeNanos)
		case constants.ResponseStatusCode:
			query = fmt.Sprintf("SELECT TIMECONVERT(\"timestamp\", 'NANOSECONDS', 'MINUTES') AS \"time\", responseStatusCode as groupBy, %s FROM %s WHERE \"timestamp\" >= %s  AND \"timestamp\" <= %s", aggregation_query, r.indexTable, startTimeNanos, endTimeNanos)

		default:
			return nil, &model.ApiError{Typ: model.ErrorBadData, Err: fmt.Errorf("groupBy type: %s not supported", queryParams.GroupBy)}
		}
	} else {
		query = fmt.Sprintf("SELECT TIMECONVERT(\"timestamp\", 'NANOSECONDS', 'MINUTES') AS \"time\", 'test', %s FROM %s WHERE \"timestamp\" >= %s  AND \"timestamp\" <= %s", aggregation_query, r.indexTable, startTimeNanos, endTimeNanos)
	}

	if len(queryParams.ServiceName) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.ServiceName, constants.ServiceName, &query, args)
	}
	if len(queryParams.HttpRoute) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.HttpRoute, constants.HttpRoute, &query, args)
	}
	if len(queryParams.HttpCode) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.HttpCode, constants.HttpCode, &query, args)
	}
	if len(queryParams.HttpHost) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.HttpHost, constants.HttpHost, &query, args)
	}
	if len(queryParams.HttpMethod) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.HttpMethod, constants.HttpMethod, &query, args)
	}
	if len(queryParams.HttpUrl) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.HttpUrl, constants.HttpUrl, &query, args)
	}
	if len(queryParams.Component) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.Component, constants.Component, &query, args)
	}
	if len(queryParams.Operation) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.Operation, constants.OperationDB, &query, args)
	}
	if len(queryParams.RPCMethod) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.RPCMethod, constants.RPCMethod, &query, args)
	}
	if len(queryParams.ResponseStatusCode) > 0 {
		args = buildFilterArrayQuery(ctx, excludeMap, queryParams.ResponseStatusCode, constants.ResponseStatusCode, &query, args)
	}
	if len(queryParams.MinDuration) != 0 {
		query = query + " AND durationNano >= " + queryParams.MinDuration
	}
	if len(queryParams.MaxDuration) != 0 {
		query = query + " AND durationNano <= " + queryParams.MaxDuration
	}
	query = getStatusFilters(query, queryParams.Status, excludeMap)

	if len(queryParams.Kind) != 0 {
		query = query + " AND kind = " + queryParams.Kind
	}

	// args, errStatus := buildQueryWithTagParams(ctx, queryParams.Tags, &query, args)
	// if errStatus != nil {
	// 	return nil, errStatus
	// }

	if queryParams.GroupBy != "" {
		query = query + " GROUP BY \"time\", groupBy ORDER BY \"time\""
	} else {
		query = query + " GROUP BY \"time\" ORDER BY \"time\""
	}

	query = query + " LIMIT " + DEFAULT_UNLIMITED_LIMIT

	rows, err := r.db.ControllerQuery(ctx, query)
	zap.S().Info(query)

	for _, row := range rows.rows {
		_r := row.([]interface{})
		SpanAggregatesDBResponseItems = append(SpanAggregatesDBResponseItems, model.SpanAggregatesDBResponseItem{
			Time:         time.Unix(0, int64(_r[0].(float64))*int64(time.Minute)),
			GroupBy:      _r[1].(string),
			Value:        uint64(_r[2].(float64)),
			FloatValue:   float32(_r[2].(float64)),
			Float64Value: _r[2].(float64),
		})
	}

	zap.S().Info(query)

	if err != nil {
		zap.S().Debug("Error in processing sql query: ", err)
		return nil, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query")}
	}

	GetFilteredSpansAggregatesResponse := model.GetFilteredSpansAggregatesResponse{
		Items: map[int64]model.SpanAggregatesResponseItem{},
	}

	for i := range SpanAggregatesDBResponseItems {
		if SpanAggregatesDBResponseItems[i].Value == 0 {
			SpanAggregatesDBResponseItems[i].Value = uint64(SpanAggregatesDBResponseItems[i].Float64Value)
		}
		SpanAggregatesDBResponseItems[i].Timestamp = int64(SpanAggregatesDBResponseItems[i].Time.UnixNano())
		SpanAggregatesDBResponseItems[i].FloatValue = float32(SpanAggregatesDBResponseItems[i].Value)
		if queryParams.AggregationOption == "rate_per_sec" {
			SpanAggregatesDBResponseItems[i].FloatValue = float32(SpanAggregatesDBResponseItems[i].Value) / float32(queryParams.StepSeconds)
		}
		if responseElement, ok := GetFilteredSpansAggregatesResponse.Items[SpanAggregatesDBResponseItems[i].Timestamp]; !ok {
			if queryParams.GroupBy != "" && SpanAggregatesDBResponseItems[i].GroupBy != "" {
				GetFilteredSpansAggregatesResponse.Items[SpanAggregatesDBResponseItems[i].Timestamp] = model.SpanAggregatesResponseItem{
					Timestamp: SpanAggregatesDBResponseItems[i].Timestamp,
					GroupBy:   map[string]float32{SpanAggregatesDBResponseItems[i].GroupBy: SpanAggregatesDBResponseItems[i].FloatValue},
				}
			} else if queryParams.GroupBy == "" {
				GetFilteredSpansAggregatesResponse.Items[SpanAggregatesDBResponseItems[i].Timestamp] = model.SpanAggregatesResponseItem{
					Timestamp: SpanAggregatesDBResponseItems[i].Timestamp,
					Value:     SpanAggregatesDBResponseItems[i].FloatValue,
				}
			}

		} else {
			if queryParams.GroupBy != "" && SpanAggregatesDBResponseItems[i].GroupBy != "" {
				responseElement.GroupBy[SpanAggregatesDBResponseItems[i].GroupBy] = SpanAggregatesDBResponseItems[i].FloatValue
			}
			GetFilteredSpansAggregatesResponse.Items[SpanAggregatesDBResponseItems[i].Timestamp] = responseElement
		}
	}

	return &GetFilteredSpansAggregatesResponse, nil
}

// SetTTL sets the TTL for traces or metrics tables.
// This is an async API which creates goroutines to set TTL.
// Status of TTL update is tracked with ttl_status table in sqlite db.
func (r *PinotReader) SetTTL(ctx context.Context,
	params *model.TTLParams) (*model.SetTTLResponseItem, *model.ApiError) {
	return nil, nil
}

func (r *PinotReader) deleteTtlTransactions(ctx context.Context, numberOfTransactionsStore int) {
	_, err := r.localDB.Exec("DELETE FROM ttl_status WHERE transaction_id NOT IN (SELECT distinct transaction_id FROM ttl_status ORDER BY created_at DESC LIMIT ?)", numberOfTransactionsStore)
	if err != nil {
		zap.S().Debug("Error in processing ttl_status delete sql query: ", err)
	}
}

// checkTTLStatusItem checks if ttl_status table has an entry for the given table name
func (r *PinotReader) checkTTLStatusItem(ctx context.Context, tableName string) (model.TTLStatusItem, *model.ApiError) {
	statusItem := []model.TTLStatusItem{}

	query := fmt.Sprintf("SELECT id, status, ttl, cold_storage_ttl FROM ttl_status WHERE table_name = '%s' ORDER BY created_at DESC", tableName)

	err := r.localDB.Select(&statusItem, query)

	zap.S().Info(query)

	if len(statusItem) == 0 {
		return model.TTLStatusItem{}, nil
	}
	if err != nil {
		zap.S().Debug("Error in processing sql query: ", err)
		return model.TTLStatusItem{}, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing ttl_status check sql query")}
	}
	return statusItem[0], nil
}

// setTTLQueryStatus fetches ttl_status table status from DB
func (r *PinotReader) setTTLQueryStatus(ctx context.Context, tableNameArray []string) (string, *model.ApiError) {
	failFlag := false
	status := constants.StatusSuccess
	for _, tableName := range tableNameArray {
		statusItem, err := r.checkTTLStatusItem(ctx, tableName)
		emptyStatusStruct := model.TTLStatusItem{}
		if statusItem == emptyStatusStruct {
			return "", nil
		}
		if err != nil {
			return "", &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing ttl_status check sql query")}
		}
		if statusItem.Status == constants.StatusPending && statusItem.UpdatedAt.Unix()-time.Now().Unix() < 3600 {
			status = constants.StatusPending
			return status, nil
		}
		if statusItem.Status == constants.StatusFailed {
			failFlag = true
		}
	}
	if failFlag {
		status = constants.StatusFailed
	}

	return status, nil
}

func (r *PinotReader) setColdStorage(ctx context.Context, tableName string, coldStorageVolume string) *model.ApiError {
	return nil
}

// GetDisks returns a list of disks {name, type} configured in clickhouse DB.
func (r *PinotReader) GetDisks(ctx context.Context) (*[]model.DiskItem, *model.ApiError) {
	return nil, &model.ApiError{model.ErrorNotImplemented, fmt.Errorf("Pinot does not support operation")}
}

// GetTTL returns current ttl, expected ttl and past setTTL status for metrics/traces.
func (r *PinotReader) GetTTL(ctx context.Context, ttlParams *model.GetTTLParams) (*model.GetTTLResponseItem, *model.ApiError) {
	return nil, &model.ApiError{model.ErrorNotImplemented, fmt.Errorf("pinot does not support setting ttl configuration")}
}

func (r *PinotReader) ListErrors(ctx context.Context, queryParams *model.ListErrorsParams) (*[]model.Error, *model.ApiError) {
	return nil, &model.ApiError{model.ErrorNotImplemented, fmt.Errorf("pinot does not support setting ttl configuration")}
}

func (r *PinotReader) CountErrors(ctx context.Context, queryParams *model.CountErrorsParams) (uint64, *model.ApiError) {
	return 0, &model.ApiError{model.ErrorNotImplemented, fmt.Errorf("pinot does not support setting ttl configuration")}
}

func (r *PinotReader) GetErrorFromErrorID(ctx context.Context, queryParams *model.GetErrorParams) (*model.ErrorWithSpan, *model.ApiError) {
	return nil, &model.ApiError{model.ErrorNotImplemented, fmt.Errorf("pinot does not support setting ttl configuration")}
}

func (r *PinotReader) GetErrorFromGroupID(ctx context.Context, queryParams *model.GetErrorParams) (*model.ErrorWithSpan, *model.ApiError) {

	var getErrorWithSpanReponse []model.ErrorWithSpan

	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE timestamp = @timestamp AND groupID = @groupID LIMIT 1", r.traceDB, r.errorTable)
	args := []interface{}{clickhouse.Named("groupID", queryParams.GroupID), clickhouse.Named("timestamp", strconv.FormatInt(queryParams.Timestamp.UnixNano(), 10))}

	err := r.db.Select(ctx, &getErrorWithSpanReponse, query, args...)

	zap.S().Info(query)

	if err != nil {
		zap.S().Debug("Error in processing sql query: ", err)
		return nil, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query")}
	}

	if len(getErrorWithSpanReponse) > 0 {
		return &getErrorWithSpanReponse[0], nil
	} else {
		return nil, &model.ApiError{Typ: model.ErrorNotFound, Err: fmt.Errorf("Error/Exception not found")}
	}

}

func (r *PinotReader) GetNextPrevErrorIDs(ctx context.Context, queryParams *model.GetErrorParams) (*model.NextPrevErrorIDs, *model.ApiError) {

	if queryParams.ErrorID == "" {
		zap.S().Debug("errorId missing from params")
		return nil, &model.ApiError{Typ: model.ErrorBadData, Err: fmt.Errorf("ErrorID missing from params")}
	}
	var err *model.ApiError
	getNextPrevErrorIDsResponse := model.NextPrevErrorIDs{
		GroupID: queryParams.GroupID,
	}
	getNextPrevErrorIDsResponse.NextErrorID, getNextPrevErrorIDsResponse.NextTimestamp, err = r.getNextErrorID(ctx, queryParams)
	if err != nil {
		zap.S().Debug("Unable to get next error ID due to err: ", err)
		return nil, err
	}
	getNextPrevErrorIDsResponse.PrevErrorID, getNextPrevErrorIDsResponse.PrevTimestamp, err = r.getPrevErrorID(ctx, queryParams)
	if err != nil {
		zap.S().Debug("Unable to get prev error ID due to err: ", err)
		return nil, err
	}
	return &getNextPrevErrorIDsResponse, nil

}

func (r *PinotReader) getNextErrorID(ctx context.Context, queryParams *model.GetErrorParams) (string, time.Time, *model.ApiError) {

	var getNextErrorIDReponse []model.NextPrevErrorIDsDBResponse

	query := fmt.Sprintf("SELECT errorID as nextErrorID, timestamp as nextTimestamp FROM %s.%s WHERE groupID = @groupID AND timestamp >= @timestamp AND errorID != @errorID ORDER BY timestamp ASC LIMIT 2", r.traceDB, r.errorTable)
	args := []interface{}{clickhouse.Named("errorID", queryParams.ErrorID), clickhouse.Named("groupID", queryParams.GroupID), clickhouse.Named("timestamp", strconv.FormatInt(queryParams.Timestamp.UnixNano(), 10))}

	err := r.db.Select(ctx, &getNextErrorIDReponse, query, args...)

	zap.S().Info(query)

	if err != nil {
		zap.S().Debug("Error in processing sql query: ", err)
		return "", time.Time{}, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query")}
	}
	if len(getNextErrorIDReponse) == 0 {
		zap.S().Info("NextErrorID not found")
		return "", time.Time{}, nil
	} else if len(getNextErrorIDReponse) == 1 {
		zap.S().Info("NextErrorID found")
		return getNextErrorIDReponse[0].NextErrorID, getNextErrorIDReponse[0].NextTimestamp, nil
	} else {
		if getNextErrorIDReponse[0].Timestamp.UnixNano() == getNextErrorIDReponse[1].Timestamp.UnixNano() {
			var getNextErrorIDReponse []model.NextPrevErrorIDsDBResponse

			query := fmt.Sprintf("SELECT errorID as nextErrorID, timestamp as nextTimestamp FROM %s.%s WHERE groupID = @groupID AND timestamp = @timestamp AND errorID > @errorID ORDER BY errorID ASC LIMIT 1", r.traceDB, r.errorTable)
			args := []interface{}{clickhouse.Named("errorID", queryParams.ErrorID), clickhouse.Named("groupID", queryParams.GroupID), clickhouse.Named("timestamp", strconv.FormatInt(queryParams.Timestamp.UnixNano(), 10))}

			err := r.db.Select(ctx, &getNextErrorIDReponse, query, args...)

			zap.S().Info(query)

			if err != nil {
				zap.S().Debug("Error in processing sql query: ", err)
				return "", time.Time{}, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query")}
			}
			if len(getNextErrorIDReponse) == 0 {
				var getNextErrorIDReponse []model.NextPrevErrorIDsDBResponse

				query := fmt.Sprintf("SELECT errorID as nextErrorID, timestamp as nextTimestamp FROM %s.%s WHERE groupID = @groupID AND timestamp > @timestamp ORDER BY timestamp ASC LIMIT 1", r.traceDB, r.errorTable)
				args := []interface{}{clickhouse.Named("errorID", queryParams.ErrorID), clickhouse.Named("groupID", queryParams.GroupID), clickhouse.Named("timestamp", strconv.FormatInt(queryParams.Timestamp.UnixNano(), 10))}

				err := r.db.Select(ctx, &getNextErrorIDReponse, query, args...)

				zap.S().Info(query)

				if err != nil {
					zap.S().Debug("Error in processing sql query: ", err)
					return "", time.Time{}, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query")}
				}

				if len(getNextErrorIDReponse) == 0 {
					zap.S().Info("NextErrorID not found")
					return "", time.Time{}, nil
				} else {
					zap.S().Info("NextErrorID found")
					return getNextErrorIDReponse[0].NextErrorID, getNextErrorIDReponse[0].NextTimestamp, nil
				}
			} else {
				zap.S().Info("NextErrorID found")
				return getNextErrorIDReponse[0].NextErrorID, getNextErrorIDReponse[0].NextTimestamp, nil
			}
		} else {
			zap.S().Info("NextErrorID found")
			return getNextErrorIDReponse[0].NextErrorID, getNextErrorIDReponse[0].NextTimestamp, nil
		}
	}
}

func (r *PinotReader) getPrevErrorID(ctx context.Context, queryParams *model.GetErrorParams) (string, time.Time, *model.ApiError) {

	var getPrevErrorIDReponse []model.NextPrevErrorIDsDBResponse

	query := fmt.Sprintf("SELECT errorID as prevErrorID, timestamp as prevTimestamp FROM %s.%s WHERE groupID = @groupID AND timestamp <= @timestamp AND errorID != @errorID ORDER BY timestamp DESC LIMIT 2", r.traceDB, r.errorTable)
	args := []interface{}{clickhouse.Named("errorID", queryParams.ErrorID), clickhouse.Named("groupID", queryParams.GroupID), clickhouse.Named("timestamp", strconv.FormatInt(queryParams.Timestamp.UnixNano(), 10))}

	err := r.db.Select(ctx, &getPrevErrorIDReponse, query, args...)

	zap.S().Info(query)

	if err != nil {
		zap.S().Debug("Error in processing sql query: ", err)
		return "", time.Time{}, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query")}
	}
	if len(getPrevErrorIDReponse) == 0 {
		zap.S().Info("PrevErrorID not found")
		return "", time.Time{}, nil
	} else if len(getPrevErrorIDReponse) == 1 {
		zap.S().Info("PrevErrorID found")
		return getPrevErrorIDReponse[0].PrevErrorID, getPrevErrorIDReponse[0].PrevTimestamp, nil
	} else {
		if getPrevErrorIDReponse[0].Timestamp.UnixNano() == getPrevErrorIDReponse[1].Timestamp.UnixNano() {
			var getPrevErrorIDReponse []model.NextPrevErrorIDsDBResponse

			query := fmt.Sprintf("SELECT errorID as prevErrorID, timestamp as prevTimestamp FROM %s.%s WHERE groupID = @groupID AND timestamp = @timestamp AND errorID < @errorID ORDER BY errorID DESC LIMIT 1", r.traceDB, r.errorTable)
			args := []interface{}{clickhouse.Named("errorID", queryParams.ErrorID), clickhouse.Named("groupID", queryParams.GroupID), clickhouse.Named("timestamp", strconv.FormatInt(queryParams.Timestamp.UnixNano(), 10))}

			err := r.db.Select(ctx, &getPrevErrorIDReponse, query, args...)

			zap.S().Info(query)

			if err != nil {
				zap.S().Debug("Error in processing sql query: ", err)
				return "", time.Time{}, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query")}
			}
			if len(getPrevErrorIDReponse) == 0 {
				var getPrevErrorIDReponse []model.NextPrevErrorIDsDBResponse

				query := fmt.Sprintf("SELECT errorID as prevErrorID, timestamp as prevTimestamp FROM %s.%s WHERE groupID = @groupID AND timestamp < @timestamp ORDER BY timestamp DESC LIMIT 1", r.traceDB, r.errorTable)
				args := []interface{}{clickhouse.Named("errorID", queryParams.ErrorID), clickhouse.Named("groupID", queryParams.GroupID), clickhouse.Named("timestamp", strconv.FormatInt(queryParams.Timestamp.UnixNano(), 10))}

				err := r.db.Select(ctx, &getPrevErrorIDReponse, query, args...)

				zap.S().Info(query)

				if err != nil {
					zap.S().Debug("Error in processing sql query: ", err)
					return "", time.Time{}, &model.ApiError{Typ: model.ErrorExec, Err: fmt.Errorf("Error in processing sql query")}
				}

				if len(getPrevErrorIDReponse) == 0 {
					zap.S().Info("PrevErrorID not found")
					return "", time.Time{}, nil
				} else {
					zap.S().Info("PrevErrorID found")
					return getPrevErrorIDReponse[0].PrevErrorID, getPrevErrorIDReponse[0].PrevTimestamp, nil
				}
			} else {
				zap.S().Info("PrevErrorID found")
				return getPrevErrorIDReponse[0].PrevErrorID, getPrevErrorIDReponse[0].PrevTimestamp, nil
			}
		} else {
			zap.S().Info("PrevErrorID found")
			return getPrevErrorIDReponse[0].PrevErrorID, getPrevErrorIDReponse[0].PrevTimestamp, nil
		}
	}
}

func (r *PinotReader) GetMetricAutocompleteTagKey(ctx context.Context, params *model.MetricAutocompleteTagParams) (*[]string, *model.ApiError) {

	// var query string
	// var err error
	// var tagKeyList []string
	// var rows driver.Rows

	// tagsWhereClause := ""

	// for key, val := range params.MetricTags {
	// 	tagsWhereClause += fmt.Sprintf(" AND labels_object.%s = '%s' ", key, val)
	// }
	// // "select distinctTagKeys from (SELECT DISTINCT arrayJoin(tagKeys) distinctTagKeys from (SELECT DISTINCT(JSONExtractKeys(labels)) tagKeys from signoz_metrics.time_series WHERE JSONExtractString(labels,'__name__')='node_udp_queues'))  WHERE distinctTagKeys ILIKE '%host%';"
	// if len(params.Match) != 0 {
	// 	query = fmt.Sprintf("select distinctTagKeys from (SELECT DISTINCT arrayJoin(tagKeys) distinctTagKeys from (SELECT DISTINCT(JSONExtractKeys(labels)) tagKeys from %s.%s WHERE metric_name=$1 %s)) WHERE distinctTagKeys ILIKE $2;", signozMetricDBName, signozTSTableName, tagsWhereClause)

	// 	rows, err = r.db.Query(ctx, query, params.MetricName, fmt.Sprintf("%%%s%%", params.Match))

	// } else {
	// 	query = fmt.Sprintf("select distinctTagKeys from (SELECT DISTINCT arrayJoin(tagKeys) distinctTagKeys from (SELECT DISTINCT(JSONExtractKeys(labels)) tagKeys from %s.%s WHERE metric_name=$1 %s ));", signozMetricDBName, signozTSTableName, tagsWhereClause)

	// 	rows, err = r.db.Query(ctx, query, params.MetricName)
	// }

	// if err != nil {
	// 	zap.S().Error(err)
	// 	return nil, &model.ApiError{Typ: model.ErrorExec, Err: err}
	// }

	// defer rows.Close()
	// var tagKey string
	// for rows.Next() {
	// 	if err := rows.Scan(&tagKey); err != nil {
	// 		return nil, &model.ApiError{Typ: model.ErrorExec, Err: err}
	// 	}
	// 	tagKeyList = append(tagKeyList, tagKey)
	// }
	return nil, nil
}

func (r *PinotReader) GetMetricAutocompleteTagValue(ctx context.Context, params *model.MetricAutocompleteTagParams) (*[]string, *model.ApiError) {

	// var query string
	// var err error
	var tagValueList []string
	// var rows driver.Rows
	// tagsWhereClause := ""

	// for key, val := range params.MetricTags {
	// 	tagsWhereClause += fmt.Sprintf(" AND labels_object.%s = '%s' ", key, val)
	// }

	// if len(params.Match) != 0 {
	// 	query = fmt.Sprintf("SELECT DISTINCT(labels_object.%s) from %s.%s WHERE metric_name=$1 %s AND labels_object.%s ILIKE $2;", params.TagKey, signozMetricDBName, signozTSTableName, tagsWhereClause, params.TagKey)

	// 	rows, err = r.db.Query(ctx, query, params.TagKey, params.MetricName, fmt.Sprintf("%%%s%%", params.Match))

	// } else {
	// 	query = fmt.Sprintf("SELECT DISTINCT(labels_object.%s) FROM %s.%s WHERE metric_name=$2 %s;", params.TagKey, signozMetricDBName, signozTSTableName, tagsWhereClause)
	// 	rows, err = r.db.Query(ctx, query, params.TagKey, params.MetricName)

	// }

	// if err != nil {
	// 	zap.S().Error(err)
	// 	return nil, &model.ApiError{Typ: model.ErrorExec, Err: err}
	// }

	// defer rows.Close()
	// var tagValue string
	// for rows.Next() {
	// 	if err := rows.Scan(&tagValue); err != nil {
	// 		return nil, &model.ApiError{Typ: model.ErrorExec, Err: err}
	// 	}
	// 	tagValueList = append(tagValueList, tagValue)
	// }

	return &tagValueList, nil
}

func (r *PinotReader) GetMetricAutocompleteMetricNames(ctx context.Context, matchText string, limit int) (*[]string, *model.ApiError) {

	// var query string
	// var err error
	var metricNameList []string
	// var rows driver.Rows

	// query = fmt.Sprintf("SELECT DISTINCT(metric_name) from %s.%s WHERE metric_name ILIKE $1", signozMetricDBName, signozTSTableName)
	// if limit != 0 {
	// 	query = query + fmt.Sprintf(" LIMIT %d;", limit)
	// }
	// rows, err = r.db.Query(ctx, query, fmt.Sprintf("%%%s%%", matchText))

	// if err != nil {
	// 	zap.S().Error(err)
	// 	return nil, &model.ApiError{Typ: model.ErrorExec, Err: err}
	// }

	// defer rows.Close()
	// var metricName string
	// for rows.Next() {
	// 	if err := rows.Scan(&metricName); err != nil {
	// 		return nil, &model.ApiError{Typ: model.ErrorExec, Err: err}
	// 	}
	// 	metricNameList = append(metricNameList, metricName)
	// }

	return &metricNameList, nil

}

// GetMetricResult runs the query and returns list of time series
func (r *PinotReader) GetMetricResult(ctx context.Context, query string) ([]*model.Series, error) {

	defer utils.Elapsed("GetMetricResult")()

	zap.S().Infof("Executing metric result query: %s", query)

	rows, err := r.db.Query(ctx, query)

	if err != nil {
		zap.S().Debug("Error in processing query: ", err)
		return nil, fmt.Errorf("error in processing query")
	}

	var (
		columnTypes = rows.ColumnTypes()
		columnNames = rows.Columns()
		vars        = make([]interface{}, len(columnTypes))
	)
	for i := range columnTypes {
		vars[i] = reflect.New(columnTypes[i].ScanType()).Interface()
	}
	// when group by is applied, each combination of cartesian product
	// of attributes is separate series. each item in metricPointsMap
	// represent a unique series.
	metricPointsMap := make(map[string][]model.MetricPoint)
	// attribute key-value pairs for each group selection
	attributesMap := make(map[string]map[string]string)

	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(vars...); err != nil {
			return nil, err
		}
		var groupBy []string
		var metricPoint model.MetricPoint
		groupAttributes := make(map[string]string)
		// Assuming that the end result row contains a timestamp, value and option labels
		// Label key and value are both strings.
		for idx, v := range vars {
			colName := columnNames[idx]
			switch v := v.(type) {
			case *string:
				// special case for returning all labels
				if colName == "fullLabels" {
					var metric map[string]string
					err := json.Unmarshal([]byte(*v), &metric)
					if err != nil {
						return nil, err
					}
					for key, val := range metric {
						groupBy = append(groupBy, val)
						groupAttributes[key] = val
					}
				} else {
					groupBy = append(groupBy, *v)
					groupAttributes[colName] = *v
				}
			case *time.Time:
				metricPoint.Timestamp = v.UnixMilli()
			case *float64:
				metricPoint.Value = *v
			}
		}
		sort.Strings(groupBy)
		key := strings.Join(groupBy, "")
		attributesMap[key] = groupAttributes
		metricPointsMap[key] = append(metricPointsMap[key], metricPoint)
	}

	var seriesList []*model.Series
	for key := range metricPointsMap {
		points := metricPointsMap[key]
		// first point in each series could be invalid since the
		// aggregations are applied with point from prev series
		if len(points) != 0 && len(points) > 1 {
			points = points[1:]
		}
		attributes := attributesMap[key]
		series := model.Series{Labels: attributes, Points: points}
		seriesList = append(seriesList, &series)
	}
	return seriesList, nil
}

func (r *PinotReader) GetTotalSpans(ctx context.Context) (uint64, error) {

	var totalSpans uint64

	queryStr := fmt.Sprintf("SELECT count() from %s.%s;", signozTraceDBName, signozTraceTableName)
	r.db.QueryRow(ctx, queryStr).Scan(&totalSpans)

	return totalSpans, nil
}

func (r *PinotReader) GetSpansInLastHeartBeatInterval(ctx context.Context) (uint64, error) {

	var spansInLastHeartBeatInterval uint64

	queryStr := fmt.Sprintf("SELECT count() from %s.%s where timestamp > toUnixTimestamp(now()-toIntervalMinute(%d));", signozTraceDBName, signozSpansTable, 30)

	r.db.QueryRow(ctx, queryStr).Scan(&spansInLastHeartBeatInterval)

	return spansInLastHeartBeatInterval, nil
}

// func sum(array []tsByMetricName) uint64 {
// 	var result uint64
// 	result = 0
// 	for _, v := range array {
// 		result += v.count
// 	}
// 	return result
// }

func (r *PinotReader) GetTimeSeriesInfo(ctx context.Context) (map[string]interface{}, error) {

	queryStr := fmt.Sprintf("SELECT count() as count from %s.%s group by metric_name order by count desc;", signozMetricDBName, signozTSTableName)

	// r.db.Select(ctx, &tsByMetricName, queryStr)

	rows, _ := r.db.Query(ctx, queryStr)

	var totalTS uint64
	totalTS = 0

	var maxTS uint64
	maxTS = 0

	count := 0
	for rows.Next() {

		var value uint64
		rows.Scan(&value)
		totalTS += value
		if count == 0 {
			maxTS = value
		}
		count += 1
	}

	timeSeriesData := map[string]interface{}{}
	timeSeriesData["totalTS"] = totalTS
	timeSeriesData["maxTS"] = maxTS

	return timeSeriesData, nil
}

func (r *PinotReader) GetSamplesInfoInLastHeartBeatInterval(ctx context.Context) (uint64, error) {

	var totalSamples uint64

	queryStr := fmt.Sprintf("select count() from %s.%s where timestamp_ms > toUnixTimestamp(now()-toIntervalMinute(%d))*1000;", signozMetricDBName, signozSampleTableName, 30)

	r.db.QueryRow(ctx, queryStr).Scan(&totalSamples)

	return totalSamples, nil
}
