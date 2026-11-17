package router

import (
	"errors"
	"fmt"
	"sort"
)

// Target is one way to serve a model: a provider plus the provider's model id and its price.
type Target struct {
	Provider      string  `json:"provider"`
	Model         string  `json:"-"`              // public model name (manifest key)
	UpstreamModel string  `json:"upstream_model"` // id sent to the provider
	InputPerMTok  float64 `json:"input_cost_per_mtok"`
	OutputPerMTok float64 `json:"output_cost_per_mtok"`
}

// Cost is the blended price used for ordering: input + output USD per 1M tokens.
func (t Target) Cost() float64 { return t.InputPerMTok + t.OutputPerMTok }

// EstimateUSD estimates the cost of a request from token usage.
func (t Target) EstimateUSD(promptTokens, completionTokens int) float64 {
	return (float64(promptTokens)*t.InputPerMTok + float64(completionTokens)*t.OutputPerMTok) / 1e6
}

// Manifest is the runtime cost-per-token map plus route aliases.
//
//	models: public model name -> providers that serve it (with price)
//	routes: alias -> public model names it may use
type Manifest struct {
	DefaultRoute string              `json:"default_route"`
	Models       map[string][]Target `json:"models"`
	Routes       map[string][]string `json:"routes"`
}

// compile validates the manifest against the configured providers and returns, for every
// requestable name (alias or model), its targets sorted cheapest first. Targets whose provider
// is not configured (e.g. no API key) are dropped; dropped is reported for logging.
func (m Manifest) compile(configured map[string]bool) (table map[string][]Target, dropped []string, err error) {
	if len(m.Routes) == 0 {
		return nil, nil, errors.New("manifest: no routes defined")
	}
	models := make(map[string][]Target, len(m.Models))
	for name, targets := range m.Models {
		if len(targets) == 0 {
			return nil, nil, fmt.Errorf("manifest: model %q has no targets", name)
		}
		for _, t := range targets {
			if t.Provider == "" || t.UpstreamModel == "" {
				return nil, nil, fmt.Errorf("manifest: model %q has a target without provider/upstream_model", name)
			}
			if t.InputPerMTok < 0 || t.OutputPerMTok < 0 {
				return nil, nil, fmt.Errorf("manifest: model %q has a negative cost", name)
			}
			if !configured[t.Provider] {
				dropped = append(dropped, name+"@"+t.Provider)
				continue
			}
			t.Model = name
			models[name] = append(models[name], t)
		}
	}

	table = make(map[string][]Target, len(models)+len(m.Routes))
	for name, ts := range models {
		table[name] = sortByCost(ts)
	}
	for alias, names := range m.Routes {
		if _, clash := m.Models[alias]; clash {
			return nil, nil, fmt.Errorf("manifest: route %q has the same name as a model", alias)
		}
		var ts []Target
		for _, n := range names {
			if _, ok := m.Models[n]; !ok {
				return nil, nil, fmt.Errorf("manifest: route %q references unknown model %q", alias, n)
			}
			ts = append(ts, models[n]...)
		}
		if len(ts) == 0 {
			return nil, nil, fmt.Errorf("manifest: route %q has no target with a configured provider", alias)
		}
		table[alias] = sortByCost(ts)
	}
	if _, ok := table[m.DefaultRoute]; !ok {
		return nil, nil, fmt.Errorf("manifest: default_route %q is not a route", m.DefaultRoute)
	}
	sort.Strings(dropped)
	return table, dropped, nil
}

// sortByCost orders cheapest first; ties keep manifest order.
func sortByCost(ts []Target) []Target {
	out := append([]Target(nil), ts...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Cost() < out[j].Cost() })
	return out
}
