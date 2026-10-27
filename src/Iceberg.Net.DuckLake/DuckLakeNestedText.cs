using System.Text;

namespace Iceberg.Net.DuckLake;

/// <summary>
/// Nested values of inlined rows. Catalogs without nested types (SQLite, PostgreSQL) hold them as DuckDB's text cast of
/// the value: lists <c>[1, NULL]</c>, structs <c>{'a': 1, 'b': x}</c>, maps <c>{k=1, 'k 2'=NULL}</c>; strings are
/// single-quoted (with backslash escapes) only when they would otherwise be ambiguous, and some primitives inside
/// structs appear as <c>CAST('2024-01-02' AS DATE)</c>. The text is not self-describing, so it is parsed against the
/// column's type, which says where each value ends. Untrusted catalog text: malformed input is a
/// <see cref="FormatException"/>.
/// </summary>
internal static class DuckLakeNestedText
{
    private const int MaxDepth = 64;

    public static object? Parse(DuckLakeType type, string text)
    {
        var reader = new Reader(text);
        var value = reader.Value(type, terminators: "", depth: 0);
        reader.SkipSpace();
        if (!reader.End) throw new FormatException($"Trailing text at {reader.Position} in '{text}'");
        return value;
    }

    private sealed class Reader(string text)
    {
        private int _i;
        public int Position => _i;
        public bool End => _i >= text.Length;

        public void SkipSpace()
        {
            while (_i < text.Length && text[_i] == ' ') _i++;
        }

        private char Peek => _i < text.Length ? text[_i] : throw new FormatException($"Unexpected end of '{text}'");

        private void Expect(char c)
        {
            SkipSpace();
            if (Peek != c) throw new FormatException($"Expected '{c}' at {_i} in '{text}'");
            _i++;
        }

        private bool TryTake(char c)
        {
            SkipSpace();
            if (_i < text.Length && text[_i] == c) { _i++; return true; }
            return false;
        }

        public object? Value(DuckLakeType type, string terminators, int depth)
        {
            if (depth > MaxDepth) throw new FormatException("Nested value is deeper than 64 levels");
            SkipSpace();
            if (IsNull(terminators)) { _i += 4; return null; }
            switch (type)
            {
                case DuckLakeList l:
                    Expect('[');
                    var items = new List<object?>();
                    if (TryTake(']')) return items;
                    do items.Add(Value(l.Element, ",]", depth + 1)); while (TryTake(','));
                    Expect(']');
                    return items;
                case DuckLakeStruct s:
                    Expect('{');
                    var fields = new object?[s.Fields.Count];
                    if (TryTake('}')) return fields;
                    do
                    {
                        var name = Token(":");
                        Expect(':');
                        var index = Index(s, name);
                        fields[index] = Value(s.Fields[index].Type, ",}", depth + 1);
                    }
                    while (TryTake(','));
                    Expect('}');
                    return fields;
                case DuckLakeMap m:
                    Expect('{');
                    var map = new Dictionary<object, object?>();
                    if (TryTake('}')) return map;
                    do
                    {
                        var key = Value(m.Key, "=", depth + 1) ?? throw new FormatException("A map key is NULL");
                        Expect('=');
                        map[key] = Value(m.Value, ",}", depth + 1);
                    }
                    while (TryTake(','));
                    Expect('}');
                    return map;
                case DuckLakePrimitive p:
                    var token = Token(terminators);
                    if (p.Name is "varchar" or "json") return token;
                    return DuckLakeValues.TryParse(p, token, out var v) ? v : throw new FormatException($"'{token}' is not a {p}");
                default:
                    throw new NotSupportedException(type.ToString());
            }
        }

        private static int Index(DuckLakeStruct s, string name)
        {
            for (var i = 0; i < s.Fields.Count; i++)
                if (s.Fields[i].Name == name) return i;
            throw new FormatException($"Struct has no field '{name}'");
        }

        private bool IsNull(string terminators)
        {
            if (string.CompareOrdinal(text, _i, "NULL", 0, 4) != 0) return false;
            var after = _i + 4;
            while (after < text.Length && text[after] == ' ') after++;
            return after >= text.Length || terminators.Contains(text[after]);
        }

        /// <summary>A quoted string, a <c>CAST('…' AS T)</c>, or unquoted text up to a terminator (trimmed).</summary>
        private string Token(string terminators)
        {
            SkipSpace();
            if (_i < text.Length && text[_i] == '\'') return Quoted();
            if (string.CompareOrdinal(text, _i, "CAST(", 0, 5) == 0)
            {
                _i += 5;
                SkipSpace();
                var inner = Quoted();
                var close = text.IndexOf(')', _i);
                if (close < 0) throw new FormatException($"Unterminated CAST in '{text}'");
                _i = close + 1;
                return inner;
            }
            var start = _i;
            while (_i < text.Length && !terminators.Contains(text[_i])) _i++;
            return text[start.._i].TrimEnd(' ');
        }

        private string Quoted()
        {
            _i++; // opening quote
            var sb = new StringBuilder();
            while (true)
            {
                if (_i >= text.Length) throw new FormatException($"Unterminated quoted string in '{text}'");
                var c = text[_i++];
                if (c == '\\' && _i < text.Length) { sb.Append(text[_i++]); continue; }
                if (c == '\'')
                {
                    if (_i < text.Length && text[_i] == '\'') { sb.Append('\''); _i++; continue; }
                    return sb.ToString();
                }
                sb.Append(c);
            }
        }
    }
}
