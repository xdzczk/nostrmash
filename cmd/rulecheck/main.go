package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

func main() {
	files := []string{
		"observability/recording_rules/slo_and_workflow_rules.yml",
		"observability/alerts/core_workflow_alerts.yml",
	}
	hasErr := false
	for _, file := range files {
		if errs := validateRuleFile(file); len(errs) > 0 {
			hasErr = true
			for _, err := range errs {
				fmt.Fprintf(os.Stderr, "%s: %v\n", file, err)
			}
		}
	}
	if hasErr {
		os.Exit(1)
	}
}

type ruleFile struct {
	Groups []ruleGroup `yaml:"groups"`
}

type ruleGroup struct {
	Name  string     `yaml:"name"`
	Rules []ruleExpr `yaml:"rules"`
}

type ruleExpr struct {
	Record string `yaml:"record"`
	Alert  string `yaml:"alert"`
	Expr   string `yaml:"expr"`
}

func validateRuleFile(path string) []error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return []error{fmt.Errorf("read file: %w", err)}
	}
	var parsed ruleFile
	if err := yaml.Unmarshal(raw, &parsed); err != nil {
		return []error{fmt.Errorf("yaml parse: %w", err)}
	}
	if len(parsed.Groups) == 0 {
		return []error{fmt.Errorf("missing groups")}
	}
	var errs []error
	for gi, group := range parsed.Groups {
		if group.Name == "" {
			errs = append(errs, fmt.Errorf("group[%d] missing name", gi))
		}
		if len(group.Rules) == 0 {
			errs = append(errs, fmt.Errorf("group[%d] has no rules", gi))
			continue
		}
		for ri, rule := range group.Rules {
			if rule.Record == "" && rule.Alert == "" {
				errs = append(errs, fmt.Errorf("group[%d].rule[%d] missing record/alert", gi, ri))
			}
			if rule.Expr == "" {
				errs = append(errs, fmt.Errorf("group[%d].rule[%d] missing expr", gi, ri))
			}
		}
	}
	return errs
}
