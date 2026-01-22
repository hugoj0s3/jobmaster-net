using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using JobMaster.Sdk.Abstractions.Config;

namespace JobMaster.SqlBase.Scripts;

public interface ISqlGenerator
{
    public string ColumnNameFor<TModel>(Expression<Func<TModel, object?>> property);
    public string ColumnTypeFor<TModel>(Expression<Func<TModel, object?>> property, 
        int? length = null, 
        bool isMaxLength = false, 
        bool nullable = true,
        int? precision = null, 
        int? scale = null);
    public string ColumnTypeFor(Type type, 
        int? length = null, 
        bool isMaxLength = false, 
        bool nullable = true,
        int? precision = null, 
        int? scale = null);
    
    public string TableNameFor<T>(string tablePrefix = "");
    
    public string TableNameFor<T>(JobMasterConfigDictionary additionalConnConfig);

    string OffsetQueryFor(int limit, int offset = 0);
    
    public string GetTablePrefix(JobMasterConfigDictionary additionalConnConfig);
    
    public string TableExistsSql(string tablePrefix, string table);
    
    public string InClauseFor(string columnName, string parameterName);

    public string NormalizeIdentifierForDb(string identifier);
    
    public string CreateIndex(string tableName, string indexName, params (string ColumnName, bool IsMaxLength, int? Length)[] columns);
    
    public string IdentityColumn();
    
    public string GenerateVersionSql();
    
    public string RepositoryTypeId { get; }
}

public abstract class SqlGenerator : ISqlGenerator
{
    protected virtual int MaxIdentifierLength => 128;

    public virtual string TableNameFor<T>(string tablePrefix = "")
    {
        var prefix = string.IsNullOrEmpty(tablePrefix) ? string.Empty : tablePrefix;
        return prefix + CamelCaseToSnakeCase(typeof(T).Name);
    }

    public virtual string TableNameFor<T>(JobMasterConfigDictionary additionalConnConfig)
    {
        var tablePrefix = GetTablePrefix(additionalConnConfig);
        return TableNameFor<T>(tablePrefix);
    }
    
    public virtual string GetTablePrefix(JobMasterConfigDictionary additionalConnConfig)
    {
        var tablePrefix = additionalConnConfig.TryGetValue<string>(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey);
        return tablePrefix ?? string.Empty;
    }

    public virtual string ColumnNameFor<TModel>(Expression<Func<TModel, object?>> property)
    {
        var memberExpr = GetMemberExpression(property.Body);
        var memberName = memberExpr?.Member.Name ?? throw new ArgumentException("Property expression expected");
        
        // Convert camelCase to snake_case
        var columnName = CamelCaseToSnakeCase(memberName);

        return columnName;
    }
    
    public virtual string ColumnTypeFor<TModel>(Expression<Func<TModel, object?>> property, 
        int? length = null, 
        bool isMaxLength = false, 
        bool nullable = true,
        int? precision = null, 
        int? scale = null)
    {
        var memberExpr = GetMemberExpression(property.Body);
        if (memberExpr == null)
        {
            throw new ArgumentException("Property expression expected");
        }
        
        var type = memberExpr.Type;
        
        return ColumnTypeFor(type, length, isMaxLength, nullable, precision, scale);
    }
    
    public virtual string ColumnTypeFor(Type type, 
        int? length = null, 
        bool isMaxLength = false, 
        bool nullable = true,
        int? precision = null, 
        int? scale = null)
    {
        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;
        if (underlyingType.IsEnum)
        {
            underlyingType = Enum.GetUnderlyingType(underlyingType);
        }
        
        if (!TypeToSqlTypeMap.TryGetValue(underlyingType, out var directTypeSql))
        {
            throw new NotSupportedException($"Type {underlyingType} is not supported");
        }
        
        var nullableString = nullable ? string.Empty : " NOT NULL";

        // Allow subclasses to provide direct mappings for common types
        if (underlyingType == typeof(string))
        {
            if (isMaxLength)
                return $"{directTypeSql}(max){nullableString}";
            
            var len = length.HasValue && length.Value > 0 ? length.Value : 255;
            return $"{directTypeSql}({len}){nullableString}";
        }

        if (underlyingType == typeof(decimal) || underlyingType == typeof(double) || underlyingType == typeof(float))
        {
            if (precision.HasValue && scale.HasValue)
            {
                return $"{directTypeSql}({precision},{scale}){nullableString}";
            }
        }
        
        return $"{directTypeSql}{nullableString}";
    }
    
    public abstract string TableExistsSql(string tablePrefix, string table);
    public abstract string InClauseFor(string columnName, string parameterName);
    public abstract string IdentityColumn();
    
    public abstract string GenerateVersionSql();

    public abstract string RepositoryTypeId { get; }
    
    public abstract string OffsetQueryFor(int limit, int offset = 0);
    
    protected abstract IDictionary<Type, string> TypeToSqlTypeMap { get; }

    private static MemberExpression? GetMemberExpression(Expression body)
    {
        // When TProp is a value type and the lambda is cast to object, compiler emits a UnaryExpression (Convert)
        return body switch
        {
            MemberExpression me => me,
            UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked, Operand: MemberExpression inner } => inner,
            _ => null
        };
    }
    
    private static string CamelCaseToSnakeCase(string value)
    {
        var sb = new StringBuilder(value.Length + 8);
        for (int i = 0; i < value.Length; i++)
        {
            char c = value[i];
            if (char.IsUpper(c))
            {
                if (i > 0 && (char.IsLower(value[i - 1]) ||
                              (i + 1 < value.Length && char.IsLower(value[i + 1]))))
                    sb.Append('_');
                sb.Append(char.ToLowerInvariant(c));
            }
            else
            {
                sb.Append(c == ' ' ? '_' : c);
            }
        }
        return sb.ToString();
    }
    
    public virtual string CreateIndex(string tableName, string indexName, params (string ColumnName, bool IsMaxLength, int? Length)[] columns)
    {
        indexName = NormalizeAndCapIdentifier(indexName, MaxIdentifierLength);
        var cols = string.Join(", ", columns.Select(c => c.ColumnName));
        return $"CREATE INDEX {indexName} ON {tableName} ({cols});";
    }

    public string NormalizeIdentifierForDb(string identifier)
    {
        return NormalizeAndCapIdentifier(identifier, MaxIdentifierLength);
    }

    protected static string NormalizeAndCapIdentifier(string value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "default";
        }

        var normalized = NormalizeIdentifier(value);

        if (maxLength <= 0)
        {
            return normalized;
        }

        if (normalized.Length <= maxLength)
        {
            return normalized;
        }

        var hash = GetShortHash(value);
        var suffix = "_" + hash;
        var allowedPrefixLength = System.Math.Max(1, maxLength - suffix.Length);
        var prefix = normalized.Substring(0, allowedPrefixLength);
        return prefix + suffix;
    }

    protected static string NormalizeIdentifier(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "default";
        }

        var chars = value.ToCharArray();
        for (var i = 0; i < chars.Length; i++)
        {
            var c = chars[i];
            if (char.IsLetterOrDigit(c) || c == '_')
            {
                continue;
            }

            chars[i] = '_';
        }

        var normalized = new string(chars);
        if (normalized.Length > 0 && char.IsDigit(normalized[0]))
        {
            normalized = "_" + normalized;
        }

        return normalized;
    }

    protected static string GetShortHash(string input)
    {
        if (string.IsNullOrEmpty(input)) return "default";

        uint hash = 0;
        foreach (char c in input)
        {
            hash = (hash << 5) - hash + c;
        }

        return hash.ToString("X8");
    }
}

public static class SqlGeneratorFactory
{
    private static readonly IDictionary<string, ISqlGenerator> RepositoryTypeIdToGeneratorTypeMap =
        new Dictionary<string, ISqlGenerator>(StringComparer.OrdinalIgnoreCase);

    private static readonly object InitLock = new();
    private static bool _initialized;

    public static void Register(ISqlGenerator generator)
    {
        if (generator == null) throw new ArgumentNullException(nameof(generator));
        lock (InitLock)
        {
            RepositoryTypeIdToGeneratorTypeMap[generator.RepositoryTypeId] = generator;
        }
    }

    public static ISqlGenerator Get(string repositoryTypeId)
    {
        if (string.IsNullOrWhiteSpace(repositoryTypeId))
            throw new ArgumentException("repositoryTypeId cannot be null or whitespace", nameof(repositoryTypeId));

        EnsureInitialized();

        var generator = TryGet(repositoryTypeId);
        if (generator != null)
            return generator;

        throw new KeyNotFoundException(
            $"No ISqlGenerator found for repositoryTypeId '{repositoryTypeId}'. Registered: " +
            string.Join(", ", RepositoryTypeIdToGeneratorTypeMap.Keys));
    }
    
    public static ISqlGenerator? TryGet(string repositoryTypeId)
    {
        EnsureInitialized();
        return RepositoryTypeIdToGeneratorTypeMap.TryGetValue(repositoryTypeId, out var generator) ? generator : null;
    }

    private static void EnsureInitialized()
    {
        if (_initialized) return;
        lock (InitLock)
        {
            if (_initialized) return;

            foreach (var gen in DiscoverGenerators())
            {
                // last one wins if duplicate RepositoryTypeId encountered
                RepositoryTypeIdToGeneratorTypeMap[gen.RepositoryTypeId] = gen;
            }

            _initialized = true;
        }
    }

    private static IEnumerable<ISqlGenerator> DiscoverGenerators()
    {
        var results = new List<ISqlGenerator>();
        var iface = typeof(ISqlGenerator);

        foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
        {
            Type[] types;
            try { types = asm.GetTypes(); }
            catch (ReflectionTypeLoadException ex)
            {
                types = ex.Types.Where(t => t != null).ToArray()!;
            }

            foreach (var t in types.Where(t => t != null && iface.IsAssignableFrom(t) && t.IsClass && !t.IsAbstract))
            {
                // require public parameterless constructor
                var ctor = t!.GetConstructor(Type.EmptyTypes);
                if (ctor == null) continue;

                try
                {
                    if (Activator.CreateInstance(t) is ISqlGenerator instance)
                    {
                        results.Add(instance);
                    }
                }
                catch
                {
                    // ignore types that fail to instantiate
                }
            }
        }

        return results;
    }
}