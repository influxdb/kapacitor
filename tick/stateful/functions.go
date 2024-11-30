package stateful

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/influxdata/influxql"
	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/zeebo/mwc"
)

var ErrNotInt = errors.New("value is not an int")

// maxArgs is used to specify the largest number of arguments that a
// builtin function can accept.
// Increment this value if you create a builtin function with more than
// the current value of maxArgs.
const (
	maxArgs = 4
)

type ErrMissingType struct {
	Name  string
	Args  []string
	Scope []string
}

func (e ErrMissingType) Error() string {
	s := "Cannot call function \"%s\" argument %s is missing, values in scope are [%s]"
	if len(e.Args) > 1 {
		s = "Cannot call function \"%s\" arguments %s are missing, values in scope are [%s]"
	}

	// remove missing values from scope
	for _, a := range e.Args {
		e.Scope = removeElement(e.Scope, a)
	}

	return fmt.Sprintf(s, e.Name, strings.Join(e.Args, ", "), strings.Join(e.Scope, ", "))
}

func removeElement(xs []string, el string) []string {
	for i, x := range xs {
		if x == el {
			xs = append(xs[:i], xs[i+1:]...)
			break
		}
	}
	return xs
}

type ErrWrongFuncSignature struct {
	Name           string
	DomainProvided Domain
	ArgLiterals    []string
	Func           Func
}

func (e ErrWrongFuncSignature) Error() string {
	var argStringer fmt.Stringer = &argDomain{args: e.ArgLiterals, domain: e.DomainProvided}
	if e.ArgLiterals == nil {
		argStringer = e.DomainProvided
	}
	return fmt.Sprintf("Cannot call function \"%s\" with args %s, available signatures are %s.",
		e.Name, argStringer, FuncDomains(e.Func))
}

type argDomain struct {
	args   []string
	domain Domain
}

func (a *argDomain) String() string {
	input := []string{}
	for j, el := range a.args {
		t := a.domain[j]
		input = append(input, fmt.Sprintf("%s: %s", el, t))
	}

	return "(" + strings.Join(input, ",") + ")"
}

var ErrNotFloat = errors.New("value is not a float")

type Domain [maxArgs]ast.ValueType

func (d Domain) String() string {
	input := []string{}
	for _, el := range d {
		if el == ast.InvalidType {
			// Because inputs should be consecutive
			break
		}
		input = append(input, el.String())
	}

	return "(" + strings.Join(input, ",") + ")"
}

type Domains []Domain

func (ds Domains) String() string {
	input := []string{}
	for _, d := range ds {
		input = append(input, d.String())
	}

	return "[" + strings.Join(input, ", ") + "]"
}

// A callable function from within the expression
type Func interface {
	Reset()
	Call(...interface{}) (interface{}, error)
	Signature() map[Domain]ast.ValueType
}

func FuncDomains(f Func) Domains {
	ds := []Domain{}

	for d := range f.Signature() {
		ds = append(ds, d)
	}

	return ds
}

// Lookup for functions
type Funcs map[string]Func

var statelessFuncs Funcs

var builtinFuncs Funcs

func init() {
	statelessFuncs = make(Funcs)
	// Conversion functions
	statelessFuncs["bool"] = boolean{}
	statelessFuncs["int"] = integer{}
	statelessFuncs["float"] = float{}
	statelessFuncs["string"] = str{}
	statelessFuncs["duration"] = duration{}

	// Math functions
	statelessFuncs["abs"] = newMath1("abs", math.Abs)
	statelessFuncs["acos"] = newMath1("acos", math.Acos)
	statelessFuncs["acosh"] = newMath1("acosh", math.Acosh)
	statelessFuncs["asin"] = newMath1("asin", math.Asin)
	statelessFuncs["asinh"] = newMath1("asinh", math.Asinh)
	statelessFuncs["atan"] = newMath1("atan", math.Atan)
	statelessFuncs["atan2"] = newMath2("atan2", math.Atan2)
	statelessFuncs["atanh"] = newMath1("atanh", math.Atanh)
	statelessFuncs["cbrt"] = newMath1("cbrt", math.Cbrt)
	statelessFuncs["ceil"] = newMath1("ceil", math.Ceil)
	statelessFuncs["cos"] = newMath1("cos", math.Cos)
	statelessFuncs["cosh"] = newMath1("cosh", math.Cosh)
	statelessFuncs["erf"] = newMath1("erf", math.Erf)
	statelessFuncs["erfc"] = newMath1("erfc", math.Erfc)
	statelessFuncs["exp"] = newMath1("exp", math.Exp)
	statelessFuncs["exp2"] = newMath1("exp2", math.Exp2)
	statelessFuncs["expm1"] = newMath1("expm1", math.Expm1)
	statelessFuncs["floor"] = newMath1("floor", math.Floor)
	statelessFuncs["gamma"] = newMath1("gamma", math.Gamma)
	statelessFuncs["hypot"] = newMath2("hypot", math.Hypot)
	statelessFuncs["j0"] = newMath1("j0", math.J0)
	statelessFuncs["j1"] = newMath1("j1", math.J1)
	statelessFuncs["jn"] = newMathIF("jn", math.Jn)
	statelessFuncs["log"] = newMath1("log", math.Log)
	statelessFuncs["log10"] = newMath1("log10", math.Log10)
	statelessFuncs["log1p"] = newMath1("log1p", math.Log1p)
	statelessFuncs["log2"] = newMath1("log2", math.Log2)
	statelessFuncs["logb"] = newMath1("logb", math.Logb)
	statelessFuncs["max"] = newMath2("max", math.Max)
	statelessFuncs["min"] = newMath2("min", math.Min)
	statelessFuncs["mod"] = newMath2("mod", math.Mod)
	statelessFuncs["pow"] = newMath2("pow", math.Pow)
	statelessFuncs["pow10"] = newMathI("pow10", math.Pow10)
	statelessFuncs["sin"] = newMath1("sin", math.Sin)
	statelessFuncs["sinh"] = newMath1("sinh", math.Sinh)
	statelessFuncs["sqrt"] = newMath1("sqrt", math.Sqrt)
	statelessFuncs["tan"] = newMath1("tan", math.Tan)
	statelessFuncs["tanh"] = newMath1("tanh", math.Tanh)
	statelessFuncs["trunc"] = newMath1("trunc", math.Trunc)
	statelessFuncs["y0"] = newMath1("y0", math.Y0)
	statelessFuncs["y1"] = newMath1("y1", math.Y1)
	statelessFuncs["yn"] = newMathIF("yn", math.Yn)

	// String functions
	statelessFuncs["strContains"] = newString2Bool("strContains", strings.Contains)
	statelessFuncs["strContainsAny"] = newString2Bool("strContainsAny", strings.ContainsAny)
	statelessFuncs["strCount"] = newString2Int("strCount", strings.Count)
	statelessFuncs["strHasPrefix"] = newString2Bool("strHasPrefix", strings.HasPrefix)
	statelessFuncs["strHasSuffix"] = newString2Bool("strHasSuffix", strings.HasSuffix)
	statelessFuncs["strIndex"] = newString2Int("strIndex", strings.Index)
	statelessFuncs["strIndexAny"] = newString2Int("strIndexAny", strings.IndexAny)
	statelessFuncs["strLastIndex"] = newString2Int("strLastIndex", strings.LastIndex)
	statelessFuncs["strLastIndexAny"] = newString2Int("strLastIndexAny", strings.LastIndexAny)
	statelessFuncs["strLength"] = strLength{}
	statelessFuncs["strReplace"] = strReplace{}
	statelessFuncs["strSubstring"] = strSubstring{}
	statelessFuncs["strToLower"] = newString1String("strToLower", strings.ToLower)
	statelessFuncs["strToUpper"] = newString1String("strToUpper", strings.ToUpper)
	statelessFuncs["strTrim"] = newString2String("strTrim", strings.Trim)
	statelessFuncs["strTrimLeft"] = newString2String("strTrimLeft", strings.TrimLeft)
	statelessFuncs["strTrimPrefix"] = newString2String("strTrimPrefix", strings.TrimPrefix)
	statelessFuncs["strTrimRight"] = newString2String("strTrimRight", strings.TrimRight)
	statelessFuncs["strTrimSpace"] = newString1String("strTrimSpace", strings.TrimSpace)
	statelessFuncs["strTrimSuffix"] = newString2String("strTrimSuffix", strings.TrimSuffix)

	// Regex functions
	statelessFuncs["regexReplace"] = regexReplace{}

	// Missing functions
	statelessFuncs["isPresent"] = isPresent{}

	// Time functions
	statelessFuncs["unixNano"] = unixNano{}
	statelessFuncs["minute"] = minute{}
	statelessFuncs["hour"] = hour{}
	statelessFuncs["weekday"] = weekday{}
	statelessFuncs["day"] = day{}
	statelessFuncs["month"] = month{}
	statelessFuncs["year"] = year{}
	statelessFuncs["now"] = now{}

	// Humanize functions
	statelessFuncs["humanBytes"] = humanBytes{}

	// Conditionals
	statelessFuncs["if"] = ifFunc{}

	// Create map of builtin functions after all functions have been added to statelessFuncs
	builtinFuncs = NewFunctions()
}

// Return set of built-in Funcs
func NewFunctions() Funcs {
	funcs := make(Funcs, len(statelessFuncs)+3)
	for n, f := range statelessFuncs {
		funcs[n] = f
	}

	// Statefull functions -- need new instance
	funcs["sigma"] = &sigma{}
	funcs["count"] = &count{}
	funcs["spread"] = &spread{min: math.Inf(+1), max: math.Inf(-1)}
	funcs["rand"] = NewRand()

	return funcs
}

type math1Func func(float64) float64
type math1 struct {
	name string
	f    math1Func
}

func newMath1(name string, f math1Func) math1 {
	return math1{
		name: name,
		f:    f,
	}
}

func (m math1) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New(m.name + " expects exactly one argument")
	}
	a0, ok := args[0].(float64)
	if !ok {
		err = fmt.Errorf("cannot pass %T to %s, must be float64", args[0], m.name)
		return
	}
	v = m.f(a0)
	return
}

var math1FuncSignature = map[Domain]ast.ValueType{}

// Initialize Math 1 Func Signature
func init() {
	d := Domain{}
	d[0] = ast.TFloat
	math1FuncSignature[d] = ast.TFloat
}

func (m math1) Signature() map[Domain]ast.ValueType {
	return math1FuncSignature
}

func (m math1) Reset() {}

type math2Func func(float64, float64) float64
type math2 struct {
	name string
	f    math2Func
}

func newMath2(name string, f math2Func) math2 {
	return math2{
		name: name,
		f:    f,
	}
}

func (m math2) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 2 {
		return 0, errors.New(m.name + " expects exactly two arguments")
	}
	a0, ok := args[0].(float64)
	if !ok {
		err = fmt.Errorf("cannot pass %T to %s, must be float64", args[0], m.name)
		return
	}
	a1, ok := args[1].(float64)
	if !ok {
		err = fmt.Errorf("cannot pass %T to %s, must be float64", args[1], m.name)
		return
	}
	v = m.f(a0, a1)
	return
}

var math2FuncSignature = map[Domain]ast.ValueType{}

// Initialize Math 2 Function Signature
func init() {
	d := Domain{}
	d[0] = ast.TFloat
	d[1] = ast.TFloat
	math2FuncSignature[d] = ast.TFloat
}

func (m math2) Signature() map[Domain]ast.ValueType {
	return math2FuncSignature
}

func (m math2) Reset() {}

type mathIFunc func(int) float64
type mathI struct {
	name string
	f    mathIFunc
}

func newMathI(name string, f mathIFunc) mathI {
	return mathI{
		name: name,
		f:    f,
	}
}

func (m mathI) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New(m.name + " expects exactly two arguments")
	}
	a0, ok := args[0].(int64)
	if !ok {
		err = fmt.Errorf("cannot pass %T to %s, must be int64", args[0], m.name)
		return
	}
	v = m.f(int(a0))
	return
}

var mathIFuncSignature = map[Domain]ast.ValueType{}

// Initialize Math I Function Signature
func init() {
	d := Domain{}
	d[0] = ast.TInt
	mathIFuncSignature[d] = ast.TFloat
}

func (m mathI) Signature() map[Domain]ast.ValueType {
	return mathIFuncSignature
}

func (m mathI) Reset() {}

type mathIFFunc func(int, float64) float64
type mathIF struct {
	name string
	f    mathIFFunc
}

func newMathIF(name string, f mathIFFunc) mathIF {
	return mathIF{
		name: name,
		f:    f,
	}
}

func (m mathIF) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 2 {
		return 0, errors.New(m.name + " expects exactly two arguments")
	}
	a0, ok := args[0].(int64)
	if !ok {
		err = fmt.Errorf("cannot pass %T to %s as first arg, must be int64", args[0], m.name)
		return
	}
	a1, ok := args[1].(float64)
	if !ok {
		err = fmt.Errorf("cannot pass %T to %s as second arg, must be float64", args[1], m.name)
		return
	}
	v = m.f(int(a0), a1)
	return
}

var mathIFFuncSignature = map[Domain]ast.ValueType{}

// Initialize Math IF Function Signature
func init() {
	d := Domain{}
	d[0] = ast.TInt
	d[1] = ast.TFloat
	mathIFFuncSignature[d] = ast.TFloat
}

func (m mathIF) Signature() map[Domain]ast.ValueType {
	return mathIFFuncSignature
}

func (m mathIF) Reset() {}

type string2BoolFunc func(string, string) bool
type string2Bool struct {
	name string
	f    string2BoolFunc
}

func newString2Bool(name string, f string2BoolFunc) string2Bool {
	return string2Bool{
		name: name,
		f:    f,
	}
}

func (m string2Bool) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 2 {
		return 0, errors.New(m.name + " expects exactly two arguments")
	}
	a0, ok := args[0].(string)
	if !ok {
		err = fmt.Errorf("cannot pass %T as first arg to %s, must be string", args[0], m.name)
		return
	}
	a1, ok := args[1].(string)
	if !ok {
		err = fmt.Errorf("cannot pass %T as second arg to %s, must be string", args[1], m.name)
		return
	}
	v = m.f(a0, a1)
	return
}

var string2BoolFuncSignature = map[Domain]ast.ValueType{}

// Initialize String 2 Bool Function Signature
func init() {
	d := Domain{}
	d[0] = ast.TString
	d[1] = ast.TString
	string2BoolFuncSignature[d] = ast.TBool
}

func (m string2Bool) Signature() map[Domain]ast.ValueType {
	return string2BoolFuncSignature
}

func (m string2Bool) Reset() {}

type string2IntFunc func(string, string) int
type string2Int struct {
	name string
	f    string2IntFunc
}

func newString2Int(name string, f string2IntFunc) string2Int {
	return string2Int{
		name: name,
		f:    f,
	}
}

func (m string2Int) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 2 {
		return 0, errors.New(m.name + " expects exactly two arguments")
	}
	a0, ok := args[0].(string)
	if !ok {
		err = fmt.Errorf("cannot pass %T as first arg to %s, must be string", args[0], m.name)
		return
	}
	a1, ok := args[1].(string)
	if !ok {
		err = fmt.Errorf("cannot pass %T as second arg to %s, must be string", args[1], m.name)
		return
	}
	v = int64(m.f(a0, a1))
	return
}

var string2IntFuncSignature = map[Domain]ast.ValueType{}

// Initialize String 2 Int Function Signature
func init() {
	d := Domain{}
	d[0] = ast.TString
	d[1] = ast.TString
	string2IntFuncSignature[d] = ast.TInt
}

func (m string2Int) Signature() map[Domain]ast.ValueType {
	return string2IntFuncSignature
}

func (m string2Int) Reset() {}

type string2StringFunc func(string, string) string
type string2String struct {
	name string
	f    string2StringFunc
}

func newString2String(name string, f string2StringFunc) string2String {
	return string2String{
		name: name,
		f:    f,
	}
}

func (m string2String) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 2 {
		return 0, errors.New(m.name + " expects exactly two arguments")
	}
	a0, ok := args[0].(string)
	if !ok {
		err = fmt.Errorf("cannot pass %T as first arg to %s, must be string", args[0], m.name)
		return
	}
	a1, ok := args[1].(string)
	if !ok {
		err = fmt.Errorf("cannot pass %T as second arg to %s, must be string", args[1], m.name)
		return
	}
	v = m.f(a0, a1)
	return
}

var string2StringFuncSignature = map[Domain]ast.ValueType{}

// Initialize String 2 String Function Signature
func init() {
	d := Domain{}
	d[0] = ast.TString
	d[1] = ast.TString
	string2StringFuncSignature[d] = ast.TString
}

func (m string2String) Signature() map[Domain]ast.ValueType {
	return string2StringFuncSignature
}

func (m string2String) Reset() {}

type string1StringFunc func(string) string
type string1String struct {
	name string
	f    string1StringFunc
}

func newString1String(name string, f string1StringFunc) string1String {
	return string1String{
		name: name,
		f:    f,
	}
}

func (m string1String) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New(m.name + " expects exactly one argument")
	}
	a0, ok := args[0].(string)
	if !ok {
		err = fmt.Errorf("cannot pass %T as first arg to %s, must be string", args[0], m.name)
		return
	}
	v = m.f(a0)
	return
}

var string1StringFuncSignature = map[Domain]ast.ValueType{}

// Initialize String 1 String Function Signature
func init() {
	d := Domain{}
	d[0] = ast.TString
	string1StringFuncSignature[d] = ast.TString
}

func (m string1String) Signature() map[Domain]ast.ValueType {
	return string1StringFuncSignature
}

func (m string1String) Reset() {}

type strLength struct {
}

func (m strLength) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New("strLength expects exactly one argument")
	}
	str, ok := args[0].(string)
	if !ok {
		err = fmt.Errorf("cannot pass %T as first arg to strLength, must be string", args[0])
		return
	}
	v = int64(len(str))
	return
}

var strLengthFuncSignature = map[Domain]ast.ValueType{}

// Initialize String Length Function Signature
func init() {
	d := Domain{}
	d[0] = ast.TString
	strLengthFuncSignature[d] = ast.TInt
}

func (strLength) Signature() map[Domain]ast.ValueType {
	return strLengthFuncSignature
}

func (m strLength) Reset() {}

type strReplace struct {
}

func (m strReplace) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 4 {
		return 0, errors.New("strReplace expects exactly four arguments")
	}
	str, ok := args[0].(string)
	if !ok {
		err = fmt.Errorf("cannot pass %T as first arg to strReplace, must be string", args[0])
		return
	}
	old, ok := args[1].(string)
	if !ok {
		err = fmt.Errorf("cannot pass %T as second arg to strReplace, must be string", args[1])
		return
	}
	new, ok := args[2].(string)
	if !ok {
		err = fmt.Errorf("cannot pass %T as third arg to strReplace, must be string", args[2])
		return
	}
	n, ok := args[3].(int64)
	if !ok {
		err = fmt.Errorf("cannot pass %T as fourth arg to strReplace, must be int", args[3])
		return
	}
	v = strings.Replace(str, old, new, int(n))
	return
}

var strReplaceFuncSignature = map[Domain]ast.ValueType{}

// Initialize String Replace Function Signature
func init() {
	d := Domain{}
	d[0] = ast.TString
	d[1] = ast.TString
	d[2] = ast.TString
	d[3] = ast.TInt
	strReplaceFuncSignature[d] = ast.TString
}

func (strReplace) Signature() map[Domain]ast.ValueType {
	return strReplaceFuncSignature
}

func (m strReplace) Reset() {}

type strSubstring struct {
}

func (m strSubstring) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 3 {
		return 0, errors.New("strSubstring expects exactly three arguments")
	}
	str, ok := args[0].(string)
	if !ok {
		err = fmt.Errorf("cannot pass %T as first arg to strSubstring, must be string", args[0])
		return
	}
	start, ok := args[1].(int64)
	if !ok {
		err = fmt.Errorf("cannot pass %T as second arg to strSubstring, must be int", args[1])
		return
	}
	stop, ok := args[2].(int64)
	if !ok {
		err = fmt.Errorf("cannot pass %T as third arg to strSubstring, must be int", args[2])
		return
	}
	if start < 0 {
		return nil, fmt.Errorf("found negative index for strSubstring: %d", start)
	}
	if stop < 0 {
		return nil, fmt.Errorf("found negative index for strSubstring: %d", stop)
	}
	if int(stop) >= len(str) {
		return nil, fmt.Errorf("stop index too large for string in strSubstring: %d", stop)
	}

	v = str[start : stop+1]
	return
}

var strSubstringFuncSignature = map[Domain]ast.ValueType{}

// Initialize String Substring Function Signature
func init() {
	d := Domain{}
	d[0] = ast.TString
	d[1] = ast.TInt
	d[2] = ast.TInt
	strSubstringFuncSignature[d] = ast.TString
}

func (strSubstring) Signature() map[Domain]ast.ValueType {
	return strSubstringFuncSignature
}

func (m strSubstring) Reset() {}

type regexReplace struct {
}

func (m regexReplace) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 3 {
		return 0, errors.New("regexReplace expects exactly three arguments")
	}
	pattern, ok := args[0].(*regexp.Regexp)
	if !ok {
		err = fmt.Errorf("cannot pass %T as first arg to regexReplace, must be regex", args[0])
		return
	}
	src, ok := args[1].(string)
	if !ok {
		err = fmt.Errorf("cannot pass %T as second arg to regexReplace, must be string", args[1])
		return
	}
	repl, ok := args[2].(string)
	if !ok {
		err = fmt.Errorf("cannot pass %T as third arg to regexReplace, must be string", args[2])
		return
	}
	v = pattern.ReplaceAllString(src, repl)
	return
}

var regexReplaceFuncSignature = map[Domain]ast.ValueType{}

// Initialize Regex Replace Function Signature
func init() {
	d := Domain{}
	d[0] = ast.TRegex
	d[1] = ast.TString
	d[2] = ast.TString
	regexReplaceFuncSignature[d] = ast.TString
}

func (regexReplace) Signature() map[Domain]ast.ValueType {
	return regexReplaceFuncSignature
}

func (m regexReplace) Reset() {}

type boolean struct {
}

func (boolean) Reset() {
}

// Converts the value to a boolean
func (boolean) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New("bool expects exactly one argument")
	}
	switch a := args[0].(type) {
	case bool:
		v = a
	case string:
		v, err = strconv.ParseBool(a)
	case int64:
		switch a {
		case 0:
			v = false
		case 1:
			v = true
		default:
			err = fmt.Errorf("cannot convert %v to boolean", a)
		}
	case float64:
		switch a {
		case 0:
			v = false
		case 1:
			v = true
		default:
			err = fmt.Errorf("cannot convert %v to boolean", a)
		}
	default:
		err = fmt.Errorf("cannot convert %T to boolean", a)
	}
	return
}

var booleanFuncSignature = map[Domain]ast.ValueType{}

// Initialize Boolean Function Signature
func init() {
	d := Domain{}
	d[0] = ast.TBool
	booleanFuncSignature[d] = ast.TBool
	d[0] = ast.TString
	booleanFuncSignature[d] = ast.TBool
	d[0] = ast.TInt
	booleanFuncSignature[d] = ast.TBool
	d[0] = ast.TFloat
	booleanFuncSignature[d] = ast.TBool
}

func (boolean) Signature() map[Domain]ast.ValueType {
	return booleanFuncSignature
}

type integer struct {
}

func (integer) Reset() {
}

// Converts the value to a integer
func (integer) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New("int expects exactly one argument")
	}
	switch a := args[0].(type) {
	case int64:
		v = a
	case float64:
		v = int64(a)
	case string:
		v, err = strconv.ParseInt(a, 10, 64)
	case bool:
		if a {
			v = int64(1)
		} else {
			v = int64(0)
		}
	case time.Duration:
		v = int64(a)
	default:
		err = fmt.Errorf("cannot convert %T to integer", a)
	}
	return
}

var integerFuncSignature = map[Domain]ast.ValueType{}

// Initialize Integer Function Signature
func init() {
	d := Domain{}
	d[0] = ast.TBool
	integerFuncSignature[d] = ast.TInt
	d[0] = ast.TString
	integerFuncSignature[d] = ast.TInt
	d[0] = ast.TInt
	integerFuncSignature[d] = ast.TInt
	d[0] = ast.TFloat
	integerFuncSignature[d] = ast.TInt
}

func (integer) Signature() map[Domain]ast.ValueType {
	return integerFuncSignature
}

type float struct {
}

func (float) Reset() {
}

// Converts the value to a float
func (float) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New("float expects exactly one argument")
	}
	switch a := args[0].(type) {
	case int64:
		v = float64(a)
	case float64:
		v = a
	case string:
		v, err = strconv.ParseFloat(a, 64)
	case bool:
		if a {
			v = float64(1)
		} else {
			v = float64(0)
		}
	default:
		err = fmt.Errorf("cannot convert %T to float", a)
	}
	return
}

var floatFuncSignature = map[Domain]ast.ValueType{}

// Initialize Float Function Signature
func init() {
	d := Domain{}
	d[0] = ast.TBool
	floatFuncSignature[d] = ast.TFloat
	d[0] = ast.TString
	floatFuncSignature[d] = ast.TFloat
	d[0] = ast.TInt
	floatFuncSignature[d] = ast.TFloat
	d[0] = ast.TFloat
	floatFuncSignature[d] = ast.TFloat
}

func (float) Signature() map[Domain]ast.ValueType {
	return floatFuncSignature
}

type str struct {
}

func (str) Reset() {
}

// Converts the value to a str
func (str) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New("string expects exactly one argument")
	}
	switch a := args[0].(type) {
	case int64:
		v = strconv.FormatInt(a, 10)
	case float64:
		v = strconv.FormatFloat(a, 'f', -1, 64)
	case bool:
		v = strconv.FormatBool(a)
	case time.Duration:
		v = influxql.FormatDuration(a)
	case string:
		v = a
	default:
		err = fmt.Errorf("cannot convert %T to string", a)
	}
	return
}

var stringFuncSignature = map[Domain]ast.ValueType{}

// Initialize String Function Signature
func init() {
	d := Domain{}
	d[0] = ast.TBool
	stringFuncSignature[d] = ast.TString
	d[0] = ast.TString
	stringFuncSignature[d] = ast.TString
	d[0] = ast.TInt
	stringFuncSignature[d] = ast.TString
	d[0] = ast.TFloat
	stringFuncSignature[d] = ast.TString
	d[0] = ast.TDuration
	stringFuncSignature[d] = ast.TString
}

func (str) Signature() map[Domain]ast.ValueType {
	return stringFuncSignature
}

type duration struct {
}

func (duration) Reset() {
}

// Converts the value to a duration in the given units
func (duration) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 && len(args) != 2 {
		return time.Duration(0), errors.New("duration expects one or two arguments duration(value, unit) where unit is optional depending on the type of value.")
	}

	getUnit := func() (time.Duration, error) {
		if len(args) != 2 {
			return 0, errors.New("duration expects unit argument for int and float values")
		}
		unit, ok := args[1].(time.Duration)
		if !ok {
			return 0, fmt.Errorf("invalid duration unit type: %T", args[1])
		}
		return unit, nil
	}
	var unit time.Duration
	switch a := args[0].(type) {
	case time.Duration:
		v = a
	case int64:
		unit, err = getUnit()
		v = time.Duration(a) * unit
	case float64:
		unit, err = getUnit()
		v = time.Duration(a * float64(unit))
	case string:
		v, err = influxql.ParseDuration(a)
		if err != nil {
			err = fmt.Errorf("invalid duration string %q", a)
		}
	default:
		err = fmt.Errorf("cannot convert %T to duration", a)
	}
	return
}

var durationFuncSignature = map[Domain]ast.ValueType{}

// Initialize Duration Function Signature
func init() {
	d := Domain{}
	d[0] = ast.TDuration
	durationFuncSignature[d] = ast.TDuration
	d[0] = ast.TInt
	d[1] = ast.TDuration
	durationFuncSignature[d] = ast.TDuration
	d[0] = ast.TFloat
	d[1] = ast.TDuration
	durationFuncSignature[d] = ast.TDuration
	d[0] = ast.TString
	d[1] = ast.TDuration
	durationFuncSignature[d] = ast.TDuration
}

func (duration) Signature() map[Domain]ast.ValueType {
	return durationFuncSignature
}

type count struct {
	n int64
}

func (c *count) Reset() {
	c.n = 0
}

// Counts the number of values processed.
func (c *count) Call(args ...interface{}) (v interface{}, err error) {
	c.n++
	return c.n, nil
}

var countFuncSignature = map[Domain]ast.ValueType{}

// Initialize Count Function Signature
func init() {
	d := Domain{}
	countFuncSignature[d] = ast.TInt
}

func (c *count) Signature() map[Domain]ast.ValueType {
	return countFuncSignature
}

// NewRand creates a new rng.  We do it this way so we can override it so tests are deterministic.
var NewRand = func() *Rand {
	return (*Rand)(mwc.Rand())
}

// rand gives you a random number.  It needs to be initalized before being run
type Rand mwc.T

func (c *Rand) Reset() {}

// returns a random number.
func (c *Rand) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) > 1 {
		return 0, errors.New("rand expects zero or one argument")
	}
	if len(args) == 1 {
		n, ok := args[0].(int64)
		if !ok {
			fmt.Println(reflect.TypeOf(args[0]).Name())
			return nil, ErrNotInt
		}
		return int64(((*mwc.T)(c).Uint64n(uint64(n)) & ((1 << 63) - 1))), nil
	}
	return int64(((*mwc.T)(c).Uint64() & ((1 << 63) - 1))), nil
}

var randFuncSignature = map[Domain]ast.ValueType{}

// Initialize Count Function Signature
func init() {
	d := Domain{}
	randFuncSignature[d] = ast.TInt
	randFuncSignature[Domain{ast.TInt}] = ast.TInt
}

func (c *Rand) Signature() map[Domain]ast.ValueType {
	return randFuncSignature
}

type sigma struct {
	mean     float64
	variance float64
	m2       float64
	n        float64
}

func (s *sigma) Reset() {
	s.mean = 0
	s.variance = 0
	s.m2 = 0
	s.n = 0
}

// Computes the number of standard devaitions a given value is from the running mean.
func (s *sigma) Call(args ...interface{}) (interface{}, error) {
	if len(args) != 1 {
		return 0, errors.New("sigma expects exactly one argument")
	}
	x, ok := args[0].(float64)
	if !ok {
		return nil, ErrNotFloat
	}
	s.n++
	delta := x - s.mean
	s.mean = s.mean + delta/s.n
	s.m2 = s.m2 + delta*(x-s.mean)
	s.variance = s.m2 / (s.n - 1)

	if s.n < 2 || s.variance == 0 {
		return float64(0), nil
	}
	return math.Abs(x-s.mean) / math.Sqrt(s.variance), nil
}

var sigmaFuncSignature = map[Domain]ast.ValueType{}

// Initialize Sigma Function Signature
func init() {
	d := Domain{}
	d[0] = ast.TFloat
	sigmaFuncSignature[d] = ast.TFloat
}

func (s *sigma) Signature() map[Domain]ast.ValueType {
	return sigmaFuncSignature
}

type spread struct {
	min float64
	max float64
}

func (s *spread) Reset() {
	s.min = math.Inf(+1)
	s.max = math.Inf(-1)
}

// Computes the running range of all values
func (s *spread) Call(args ...interface{}) (interface{}, error) {
	if len(args) != 1 {
		return 0, errors.New("spread expects exactly one argument")
	}
	x, ok := args[0].(float64)
	if !ok {
		return nil, ErrNotFloat
	}

	if x < s.min {
		s.min = x
	}

	if x > s.max {
		s.max = x
	}

	return s.max - s.min, nil
}

var spreadFuncSignature = map[Domain]ast.ValueType{}

// Initialize Spread Function Signature
func init() {
	d := Domain{}
	d[0] = ast.TFloat
	spreadFuncSignature[d] = ast.TFloat
}

func (s *spread) Signature() map[Domain]ast.ValueType {
	return spreadFuncSignature
}

// Time function signatures
var timeFuncSignature = map[Domain]ast.ValueType{}

// Initialize Time Function Signature
func init() {
	d := Domain{}
	d[0] = ast.TTime
	timeFuncSignature[d] = ast.TInt
}

type unixNano struct {
}

func (unixNano) Reset() {
}

// Return the nanosecond unix timestamp for the given time.
func (unixNano) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New("unixNano expects exactly one argument")
	}
	switch a := args[0].(type) {
	case time.Time:
		v = int64(a.UnixNano())
	default:
		err = fmt.Errorf("cannot convert %T to time.Time", a)
	}
	return
}

func (unixNano) Signature() map[Domain]ast.ValueType {
	return timeFuncSignature
}

type minute struct {
}

func (minute) Reset() {
}

// Return the minute within the hour for the given time, within the range [0,59].
func (minute) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New("minute expects exactly one argument")
	}
	switch a := args[0].(type) {
	case time.Time:
		v = int64(a.Minute())
	default:
		err = fmt.Errorf("cannot convert %T to time.Time", a)
	}
	return
}

func (minute) Signature() map[Domain]ast.ValueType {
	return timeFuncSignature
}

type hour struct {
}

func (hour) Reset() {
}

// Return the hour within the day for the given time, within the range [0,23].
func (hour) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New("hour expects exactly one argument")
	}
	switch a := args[0].(type) {
	case time.Time:
		v = int64(a.Hour())
	default:
		err = fmt.Errorf("cannot convert %T to time.Time", a)
	}
	return
}

func (hour) Signature() map[Domain]ast.ValueType {
	return timeFuncSignature
}

type weekday struct {
}

func (weekday) Reset() {
}

// Return the weekday within the week for the given time, within the range [0,6] where 0 is Sunday.
func (weekday) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New("weekday expects exactly one argument")
	}
	switch a := args[0].(type) {
	case time.Time:
		v = int64(a.Weekday())
	default:
		err = fmt.Errorf("cannot convert %T to time.Time", a)
	}
	return
}

func (weekday) Signature() map[Domain]ast.ValueType {
	return timeFuncSignature
}

type day struct {
}

func (day) Reset() {
}

// Return the day within the month for the given time, within the range [1,31] depending on the month.
func (day) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New("day expects exactly one argument")
	}
	switch a := args[0].(type) {
	case time.Time:
		v = int64(a.Day())
	default:
		err = fmt.Errorf("cannot convert %T to time.Time", a)
	}
	return
}

func (day) Signature() map[Domain]ast.ValueType {
	return timeFuncSignature
}

type month struct {
}

func (month) Reset() {
}

// Return the month within the year for the given time, within the range [1,12].
func (month) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New("month expects exactly one argument")
	}
	switch a := args[0].(type) {
	case time.Time:
		v = int64(a.Month())
	default:
		err = fmt.Errorf("cannot convert %T to time.Time", a)
	}
	return
}

func (month) Signature() map[Domain]ast.ValueType {
	return timeFuncSignature
}

type year struct {
}

func (year) Reset() {
}

// Return the year for the given time.
func (year) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New("year expects exactly one argument")
	}
	switch a := args[0].(type) {
	case time.Time:
		v = int64(a.Year())
	default:
		err = fmt.Errorf("cannot convert %T to time.Time", a)
	}
	return
}

func (year) Signature() map[Domain]ast.ValueType {
	return timeFuncSignature
}

var nowFuncSignature = map[Domain]ast.ValueType{}

// Initialize Now function signature
func init() {
	d := Domain{}
	nowFuncSignature[d] = ast.TTime
}

type now struct {
}

func (now) Reset() {
}

// Return the current local time.
func (now) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 0 {
		return 0, errors.New("now expects exactly zero argument")
	}
	v = time.Now()

	return
}

func (now) Signature() map[Domain]ast.ValueType {
	return nowFuncSignature
}

type humanBytes struct {
}

func (humanBytes) Reset() {

}

func (humanBytes) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return 0, errors.New("humanBytes expects exactly one argument")
	}
	switch a := args[0].(type) {
	case float64:
		v = humanize.Bytes(uint64(a))
	case int64:
		v = humanize.Bytes(uint64(a))
	default:
		err = fmt.Errorf("cannot convert %T to humanBytes", a)
	}
	return
}

var humanBytesFuncSignature = map[Domain]ast.ValueType{}

// Initialize HumanBytes Function Signature
func init() {
	d := Domain{}
	d[0] = ast.TFloat
	humanBytesFuncSignature[d] = ast.TString
	d[0] = ast.TInt
	humanBytesFuncSignature[d] = ast.TString
}

func (humanBytes) Signature() map[Domain]ast.ValueType {
	return humanBytesFuncSignature
}

type ifFunc struct {
}

func (ifFunc) Reset() {

}

func (ifFunc) Call(args ...interface{}) (interface{}, error) {
	if len(args) != 3 {
		return nil, errors.New("if expects exactly three arguments")
	}

	var condition bool
	var isBool bool

	if condition, isBool = args[0].(bool); !isBool {
		return nil, fmt.Errorf("first argument to if must be a condition with type of bool - got %T", args[0])
	}

	if reflect.TypeOf(args[1]) != reflect.TypeOf(args[2]) {
		return nil, fmt.Errorf("Different return types are not supported - second argument is %T and third argument is %T", args[1], args[2])
	}

	if condition {
		return args[1], nil
	}

	return args[2], nil
}

var ifFuncSignature = map[Domain]ast.ValueType{}

// Initialize If Function Signature
func init() {
	d := Domain{}
	d[0] = ast.TBool
	types := []ast.ValueType{
		ast.TFloat,
		ast.TInt,
		ast.TString,
		ast.TBool,
		ast.TRegex,
		ast.TTime,
		ast.TDuration,
	}

	for _, t := range types {
		d[1] = t
		d[2] = t
		ifFuncSignature[d] = t
	}
}

func (ifFunc) Signature() map[Domain]ast.ValueType {
	return ifFuncSignature
}

type isPresent struct {
}

func (isPresent) Reset() {

}

func (isPresent) Call(args ...interface{}) (v interface{}, err error) {
	if len(args) != 1 {
		return false, errors.New("isMissing expects exactly one argument")
	}
	_, isMissing := args[0].(*ast.Missing)

	return !isMissing, nil
}

var isPresentFuncSignature = map[Domain]ast.ValueType{}

// Initialize isPresent Function Signature
func init() {
	d := Domain{}
	d[0] = ast.TMissing
	isPresentFuncSignature[d] = ast.TBool
	d[0] = ast.TBool
	isPresentFuncSignature[d] = ast.TBool
	d[0] = ast.TString
	isPresentFuncSignature[d] = ast.TBool
	d[0] = ast.TInt
	isPresentFuncSignature[d] = ast.TBool
	d[0] = ast.TFloat
	isPresentFuncSignature[d] = ast.TBool
}

func (isPresent) Signature() map[Domain]ast.ValueType {
	return isPresentFuncSignature
}
