package http

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"

	platform "github.com/influxdata/influxdb"
	pcontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/http/metric"
	"github.com/influxdata/influxdb/kit/tracing"
	"github.com/influxdata/influxdb/query/control"
	jsoniter "github.com/json-iterator/go"
	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/httputil"
	"github.com/prometheus/prometheus/util/stats"
	"go.uber.org/zap"
)

// PromAPIBackend is all services and associated parameters required to construct
// the PromAPIHandler.
type PromAPIBackend struct {
	Logger             *zap.Logger
	QueryEventRecorder metric.EventRecorder

	QueryController     *control.Controller
	OrganizationService platform.OrganizationService
}

// NewPromAPIBackend returns a new instance of PromAPIBackend.
func NewPromAPIBackend(b *APIBackend) *PromAPIBackend {
	return &PromAPIBackend{
		Logger:             b.Logger.With(zap.String("handler", "prom_api")),
		QueryEventRecorder: b.QueryEventRecorder,

		QueryController:     b.FluxQueryController,
		OrganizationService: b.OrganizationService,
	}
}

// PromAPIHandler implements handling flux queries.
type PromAPIHandler struct {
	*httprouter.Router

	Logger *zap.Logger

	OrganizationService platform.OrganizationService
	QueryController     *control.Controller

	EventRecorder metric.EventRecorder
}

// NewPromAPIHandler returns a new handler at /api/v2/prometheus for Prometheus API queries.
func NewPromAPIHandler(b *PromAPIBackend) *PromAPIHandler {
	h := &PromAPIHandler{
		Router: NewRouter(),
		Logger: b.Logger,

		QueryController:     b.QueryController,
		OrganizationService: b.OrganizationService,
		EventRecorder:       b.QueryEventRecorder,
	}

	corsOrigin := regexp.MustCompile(".*")

	wrap := func(f apiFunc) http.HandlerFunc {
		hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			httputil.SetCORS(w, corsOrigin, r)
			result := f(r)
			if result.err != nil {
				h.respondError(w, result.err, result.data)
			} else if result.data != nil {
				h.respond(w, result.data)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		})
		return httputil.CompressionHandler{
			Handler: hf,
		}.ServeHTTP
	}

	h.HandlerFunc("GET", "/api/v2/prometheus/:org/:bucket/api/v1/query_range", wrap(h.handleQueryRange))
	return h
}

func (h *PromAPIHandler) respond(w http.ResponseWriter, data interface{}) {
	statusMessage := statusSuccess
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	b, err := json.Marshal(&response{
		Status: statusMessage,
		Data:   data,
	})
	if err != nil {
		h.Logger.Info("Error marshaling JSON response",
			zap.String("handler", "prom_api"),
			zap.Error(err),
		)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if n, err := w.Write(b); err != nil {
		h.Logger.Info("Error writing response",
			zap.String("handler", "prom_api"),
			zap.Int("bytes_written", n),
			zap.Error(err),
		)
	}
}

func (h *PromAPIHandler) respondError(w http.ResponseWriter, apiErr *apiError, data interface{}) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	b, err := json.Marshal(&response{
		Status:    statusError,
		ErrorType: apiErr.typ,
		Error:     apiErr.err.Error(),
		Data:      data,
	})
	if err != nil {
		h.Logger.Info("Error marshaling JSON response",
			zap.String("handler", "prom_api"),
			zap.Error(err),
		)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var code int
	switch apiErr.typ {
	case errorBadData:
		code = http.StatusBadRequest
	case errorExec:
		code = 422
	case errorCanceled, errorTimeout:
		code = http.StatusServiceUnavailable
	case errorInternal:
		code = http.StatusInternalServerError
	case errorNotFound:
		code = http.StatusNotFound
	default:
		code = http.StatusInternalServerError
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if n, err := w.Write(b); err != nil {
		h.Logger.Info("Error writing response",
			zap.String("handler", "prom_api"),
			zap.Int("bytes_written", n),
			zap.Error(err),
		)
	}
}

type status string

const (
	statusSuccess status = "success"
	statusError   status = "error"
)

type errorType string

const (
	errorNone        errorType = ""
	errorTimeout     errorType = "timeout"
	errorCanceled    errorType = "canceled"
	errorExec        errorType = "execution"
	errorBadData     errorType = "bad_data"
	errorInternal    errorType = "internal"
	errorUnavailable errorType = "unavailable"
	errorNotFound    errorType = "not_found"
)

type apiError struct {
	typ errorType
	err error
}

func (e *apiError) Error() string {
	return fmt.Sprintf("%s: %s", e.typ, e.err)
}

type response struct {
	Status    status      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType errorType   `json:"errorType,omitempty"`
	Error     string      `json:"error,omitempty"`
	Warnings  []string    `json:"warnings,omitempty"`
}

type apiFuncResult struct {
	data interface{}
	err  *apiError
}

type apiFunc func(r *http.Request) apiFuncResult

type queryData struct {
	ResultType promql.ValueType  `json:"resultType"`
	Result     promql.Value      `json:"result"`
	Stats      *stats.QueryStats `json:"stats,omitempty"`
}

func (h *PromAPIHandler) handleQueryRange(r *http.Request) apiFuncResult {
	span, r := tracing.ExtractFromHTTPRequest(r, "PromAPIHandler")
	defer span.Finish()

	ctx := r.Context()

	a, err := pcontext.GetAuthorizer(ctx)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, errors.New("authorization is invalid or missing in the query request")}}
	}

	_ = a

	params := httprouter.ParamsFromContext(ctx)
	orgName := params.ByName("org")
	org, err := h.OrganizationService.FindOrganization(ctx, platform.OrganizationFilter{
		Name: &orgName,
	})
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, err}}
	}

	_ = org

	return apiFuncResult{&queryData{
		// ResultType: res.Value.Type(),
		// Result:     res.Value,
		// Stats:      qs,
	}, nil}
}

// func (h *PromAPIHandler) handle(w http.ResponseWriter, r *http.Request) {
// 	const op = "http/handlePromAPIQuery"
// 	span, r := tracing.ExtractFromHTTPRequest(r, "FluxHandler")
// 	defer span.Finish()

// 	ctx := r.Context()
// 	promAPI := v1.NewAPI(
// 		nil,
// 		nil,
// 		dummyTargetRetriever{},
// 		dummyAlertmanagerRetriever{},
// 		func() config.Config { return config.Config{} },
// 		map[string]string{}, // TODO: include configuration flags
// 		func(f http.HandlerFunc) http.HandlerFunc { return f },
// 		func() v1.TSDBAdmin { return nil }, // Only needed for admin APIs.
// 		false, // Disable admin APIs.
// 		nil,
// 		nil,
// 		0,
// 		0,
// 		nil
// 	)
// 	// TODO: Ultra-hack with the prefix.
// 	rtr := route.New().WithPrefix(fmt.Sprintf("/api/v2/prometheus/%s/%s/api/v1", r.FormValue("org"), r.FormValue("bucket")))
// 	promAPI.Register(rtr)
// 	rtr.ServeHTTP(w, r)
// 	// - req comes in
// 	// - extract org + bucket from req
// 	// - engine = newFluxPromQLEngine(bucket, org)
// 	// - api = v1.NewAPI(nil, engine)
// 	// - router = route.New(...)
// 	// - api.Register(router)

// 	// - Flux PromQL Engine:
// 	//   - q = engine.NewRangeQuery(): parse/validate PromQL, transpile to Flux
// 	//   - q.Exec(): run Flux query <-- This is where the Flux backend is needed
// }

// // dummyTargetRetriever implements github.com/prometheus/prometheus/web/api/v1.targetRetriever.
// type dummyTargetRetriever struct{}

// // TargetsActive implements targetRetriever.
// func (dummyTargetRetriever) TargetsActive() map[string][]*scrape.Target {
// 	return map[string][]*scrape.Target{}
// }

// // TargetsDropped implements targetRetriever.
// func (dummyTargetRetriever) TargetsDropped() map[string][]*scrape.Target {
// 	return map[string][]*scrape.Target{}
// }

// // dummyAlertmanagerRetriever implements AlertmanagerRetriever.
// type dummyAlertmanagerRetriever struct{}

// // Alertmanagers implements AlertmanagerRetriever.
// func (dummyAlertmanagerRetriever) Alertmanagers() []*url.URL { return nil }

// // DroppedAlertmanagers implements AlertmanagerRetriever.
// func (dummyAlertmanagerRetriever) DroppedAlertmanagers() []*url.URL { return nil }

// // dummyRulesRetriever implements RulesRetriever.
// type dummyRulesRetriever struct{}

// // RuleGroups implements RulesRetriever.
// func (dummyRulesRetriever) RuleGroups() []*rules.Group {
// 	return nil
// }

// // AlertingRules implements RulesRetriever.
// func (dummyRulesRetriever) AlertingRules() []*rules.AlertingRule {
// 	return nil
// }
