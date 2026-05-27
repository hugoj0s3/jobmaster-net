# Builder Review

Your progress on `JobMasterDashboardBuilder` is great! You've cleanly removed the nested action pattern and the code is looking exactly like a modern fluent builder. 

```csharp
    public IJobMasterDashboardThemeSelector AddTheme(DashboardBuiltInTheme theme, string? displayName = null)
    {
        var config = new DashboardThemeItemConfig
        {
            BaseTheme = theme,
            DisplayName = displayName ?? theme.ToString()
        };
        options.Themes.Themes.Add(config);
        return new DashboardThemeBuilder(options, config); // Excellent!
    }
```

There is just **one technical C# trap** we need to address with the interface inheritance you requested:

You mentioned:
> `IJobMasterDashboardPrimaryThemeSelector` is also `IJobMasterDashboardThemeSelector` so we can mark as default in cluster.

If `IJobMasterDashboardPrimaryThemeSelector` simply inherits from `IJobMasterDashboardThemeSelector`, then the color methods (like `.Primary()`) will return `IJobMasterDashboardThemeSelector`, which breaks the fluent chain for primary-specific methods:

```csharp
builder.AddPrimaryTheme(DashboardBuiltInTheme.Corporate)
       .Primary("#FFF") // <--- Returns IJobMasterDashboardThemeSelector!
       .SetFontFamily("Inter"); // <--- COMPILER ERROR! SetFontFamily doesn't exist on standard selector.
```

### The Solution
We have a few options to solve this without making the code confusing. 

**Option A: The `new` keyword (Recommended - Simplest Interfaces)**
We can get rid of the generic `TReturn` completely and use `new` to "shadow" the return types in the primary selector interface.

```csharp
public interface IJobMasterDashboardThemeSelector
{
    IJobMasterDashboardThemeSelector Primary(string color, string content = null);
    IJobMasterDashboardThemeSelector DefaultForClusterId(params string[] clusterIds);
    IJobMasterDashboardPrimaryThemeSelector MakePrimary();
}

public interface IJobMasterDashboardPrimaryThemeSelector : IJobMasterDashboardThemeSelector
{
    // Shadow the methods so they return the primary selector instead!
    new IJobMasterDashboardPrimaryThemeSelector Primary(string color, string content = null);
    new IJobMasterDashboardPrimaryThemeSelector DefaultForClusterId(params string[] clusterIds);
    
    // Primary-specific methods
    IJobMasterDashboardPrimaryThemeSelector SetFontFamily(string font);
}
```

Then, your `DashboardThemeBuilder` class just implicitly implements both, or explicitly implements the base interface if necessary, returning `this`.

**Option B: Keep Generics using CRTP**
This involves complex generic constraints (`IJobMasterDashboardThemeSelector<T> where T : IJobMasterDashboardThemeSelector<T>`). It works perfectly for chaining but makes the interface definitions look extremely messy.

### Next Step
Since you are driving the implementation of `JobMasterDashboardBuilder`, do you want me to write the `DashboardThemeBuilder` class and adjust the interfaces using **Option A** so your chain works flawlessly?
