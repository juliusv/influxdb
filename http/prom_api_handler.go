// Much of the code below has been adapted from Prometheus web API code at:
// https://github.com/prometheus/prometheus/blob/a000cec011b87fee6576b88cbcf12b734cf0d139/web/api/v1/api.go
//
// The original Prometheus license notice applies for these parts:
//
// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package http

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/promflux"
	influxdb "github.com/influxdata/influxdb"
	platform "github.com/influxdata/influxdb"
	pcontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/http/metric"
	"github.com/influxdata/influxdb/kit/tracing"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/query/control"
	jsoniter "github.com/json-iterator/go"
	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/httputil"
	"go.uber.org/zap"
)

type promStatus string

const (
	promStatusSuccess promStatus = "success"
	promStatusError   promStatus = "error"
)

type promErrorType string

const (
	promErrorNone        promErrorType = ""
	promErrorTimeout     promErrorType = "timeout"
	promErrorCanceled    promErrorType = "canceled"
	promErrorExec        promErrorType = "execution"
	promErrorBadData     promErrorType = "bad_data"
	promErrorInternal    promErrorType = "internal"
	promErrorUnavailable promErrorType = "unavailable"
	promErrorNotFound    promErrorType = "not_found"
)

type promAPIError struct {
	typ promErrorType
	err error
}

func (e *promAPIError) Error() string {
	return fmt.Sprintf("%s: %s", e.typ, e.err)
}

type response struct {
	Status    promStatus    `json:"status"`
	Data      interface{}   `json:"data,omitempty"`
	ErrorType promErrorType `json:"errorType,omitempty"`
	Error     string        `json:"error,omitempty"`
	Warnings  []string      `json:"warnings,omitempty"`
}

type promAPIFuncResult struct {
	data interface{}
	err  *promAPIError
}

type promAPIFunc func(r *http.Request) promAPIFuncResult

type promQueryData struct {
	ResultType promql.ValueType `json:"resultType"`
	Result     promql.Value     `json:"result"`
}

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

	wrap := func(f promAPIFunc) http.HandlerFunc {
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
	statusMessage := promStatusSuccess
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

func (h *PromAPIHandler) respondError(w http.ResponseWriter, apiErr *promAPIError, data interface{}) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	b, err := json.Marshal(&response{
		Status:    promStatusError,
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
	case promErrorBadData:
		code = http.StatusBadRequest
	case promErrorExec:
		code = 422
	case promErrorCanceled, promErrorTimeout:
		code = http.StatusServiceUnavailable
	case promErrorInternal:
		code = http.StatusInternalServerError
	case promErrorNotFound:
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

func (h *PromAPIHandler) handleQueryRange(r *http.Request) promAPIFuncResult {
	span, r := tracing.ExtractFromHTTPRequest(r, "PromAPIHandler")
	defer span.Finish()

	req, timeout, apiErr := decodeQueryRangeRequest(r, h.OrganizationService)
	if apiErr != nil {
		return promAPIFuncResult{nil, &promAPIError{promErrorBadData, fmt.Errorf("error decoding range query request: %s", apiErr)}}
	}

	ctx := r.Context()
	if timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	q, err := h.QueryController.Query(ctx, req)
	if err != nil {
		return promAPIFuncResult{nil, &promAPIError{promErrorExec, fmt.Errorf("error executing query: %s", err)}}
	}

	return encodeRangeQueryResult(q)
}

func decodeQueryRangeRequest(r *http.Request, orgSvc platform.OrganizationService) (*query.Request, time.Duration, *promAPIError) {
	// 1. Decode and sanity-check PromQL-specific query parameters.
	start, err := parseTime(r.FormValue("start"))
	if err != nil {
		return nil, 0, &promAPIError{promErrorBadData, fmt.Errorf("invalid parameter 'start': %s", err)}
	}
	end, err := parseTime(r.FormValue("end"))
	if err != nil {
		return nil, 0, &promAPIError{promErrorBadData, fmt.Errorf("invalid parameter 'end': %s", err)}
	}
	if end.Before(start) {
		return nil, 0, &promAPIError{promErrorBadData, errors.New("end timestamp must not be before start time")}
	}

	step, err := parseDuration(r.FormValue("step"))
	if err != nil {
		return nil, 0, &promAPIError{promErrorBadData, fmt.Errorf("invalid parameter 'step': %s", err)}
	}

	if step <= 0 {
		return nil, 0, &promAPIError{promErrorBadData, errors.New("zero or negative query resolution step widths are not accepted. Try a positive integer")}
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if end.Sub(start)/step > 11000 {
		return nil, 0, &promAPIError{promErrorBadData, errors.New("exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")}
	}

	var timeout time.Duration
	if to := r.FormValue("timeout"); to != "" {
		timeout, err = parseDuration(to)
		if err != nil {
			return nil, 0, &promAPIError{promErrorBadData, fmt.Errorf("invalid parameter 'timeout': %s", err)}
		}
	}

	expr, err := promql.ParseExpr(r.FormValue("query"))
	if err != nil {
		return nil, 0, &promAPIError{promErrorBadData, fmt.Errorf("error parsing PromQL expression: %s", err)}
	}

	// 2. Decode extra Flux-specific parameters (auth, org, and bucket).
	ctx := r.Context()
	a, err := pcontext.GetAuthorizer(ctx)
	if err != nil {
		return nil, 0, &promAPIError{promErrorBadData, errors.New("authorization is invalid or missing in the query request")}
	}

	params := httprouter.ParamsFromContext(ctx)

	orgName := params.ByName("org")
	org, err := orgSvc.FindOrganization(ctx, platform.OrganizationFilter{
		Name: &orgName,
	})
	if err != nil {
		return nil, 0, &promAPIError{promErrorBadData, fmt.Errorf("couldn't find organization %q: %s", err, org)}
	}

	var auth *influxdb.Authorization
	switch t := a.(type) {
	case *influxdb.Authorization:
		auth = t
	case *influxdb.Session:
		auth = t.EphemeralAuth(org.ID)
	default:
		return nil, 0, &promAPIError{promErrorBadData, influxdb.ErrAuthorizerNotSupported}
	}

	bucket := params.ByName("bucket")

	// 3. Transpile PromQL -> Flux.
	tr := &promflux.Transpiler{
		Bucket:     bucket,
		Start:      start,
		End:        end.UTC(),
		Resolution: step,
	}

	fluxFile, err := tr.Transpile(expr)
	if err != nil {
		return nil, 0, &promAPIError{promErrorInternal, fmt.Errorf("error transpiling query: %s", err)}
	}

	req := &query.Request{
		Authorization:  auth,
		OrganizationID: org.ID,
		Compiler: lang.ASTCompiler{
			AST: &ast.Package{Package: "main", Files: []*ast.File{fluxFile}},
			Now: time.Now(),
		},
	}

	return req, timeout, nil
}

func encodeRangeQueryResult(q flux.Query) promAPIFuncResult {
	results := flux.NewResultIteratorFromQuery(q)
	defer results.Release()

	if !results.More() {
		return promAPIFuncResult{nil, &promAPIError{promErrorInternal, fmt.Errorf("Flux result iterator didn't contain any result")}}
	}
	res := results.Next()
	if err := results.Err(); err != nil {
		return promAPIFuncResult{nil, &promAPIError{promErrorInternal, fmt.Errorf("error getting Flux result: %s", err)}}
	}
	if results.More() {
		return promAPIFuncResult{nil, &promAPIError{promErrorInternal, fmt.Errorf("Flux result iterator contained more than one result")}}
	}

	promRes := promflux.InfluxResultToPromMatrix(res)
	return promAPIFuncResult{
		&promQueryData{
			ResultType: promRes.Type(),
			Result:     promRes,
		},
		nil,
	}
}

func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		return time.Unix(int64(s), int64(ns*float64(time.Second))), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}
