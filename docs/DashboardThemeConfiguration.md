# Dashboard Theme Configuration

The JobMaster Dashboard supports full theme customization through the fluent builder API. Themes control colors, border radii, and font families. Each cluster can have its own theme, and one theme must be marked as the primary (fallback) theme.

---

## Registering Themes

Themes are registered inside `AddJobMasterDashboard` using the `.AddTheme()` builder:

```csharp
builder.Services.AddJobMasterDashboard(options =>
{
    options.AddTheme("My Light Theme", baseTheme: "jobmaster-light")
        .MakePrimary()
        .DefaultForClusterId("prod-cluster");

    options.AddTheme("My Dark Theme", baseTheme: "jobmaster-dark")
        .DefaultForClusterId("staging-cluster");
});
```

`MakePrimary()` designates the fallback theme used when no cluster-specific theme is resolved.

---

## Font Configuration

The dashboard ships with **Nunito Variable** (sans-serif) and **JetBrains Mono Variable** (monospace) bundled as self-hosted assets — no network request required by default.

### Bundled fonts (no URL needed)

If you only want to change the font family to another font that is already available as a system font or that you load yourself, pass just the family stack:

```csharp
options.AddTheme("Custom", baseTheme: "jobmaster-light")
    .SetFontSans(["system-ui", "ui-sans-serif"])
    .SetFontMono(["ui-monospace", "monospace"]);
```

### External fonts (Google Fonts, Bunny Fonts, self-hosted)

If the font is not bundled, pass its stylesheet URL as the second argument. The dashboard injects a `<link rel="stylesheet">` into the page at runtime so the font loads before it is applied.

```csharp
options.AddTheme("Geist Theme", baseTheme: "jobmaster-light")
    .SetFontSans(
        ["Geist", "ui-sans-serif"],
        fontUrl: "https://fonts.googleapis.com/css2?family=Geist:wght@300;400;800&display=swap"
    )
    .SetFontMono(
        ["Fira Code", "monospace"],
        fontUrl: "https://fonts.googleapis.com/css2?family=Fira+Code:wght@400;700&display=swap"
    );
```

> **Privacy note:** Using Google Fonts URLs means the end user's browser makes a request to Google's servers. For GDPR-sensitive deployments, prefer [Bunny Fonts](https://fonts.bunny.net) (a drop-in replacement) or self-hosted font files.

### Font stack rules

- The first entry in the array is the primary font. Subsequent entries are fallbacks.
- Always end the sans stack with `"ui-sans-serif"` or `"system-ui"` and the mono stack with `"monospace"` so the browser has a safe fallback.
- Injected font URLs are deduplicated — switching between themes that share the same URL will not insert duplicate `<link>` tags.

---

## Color Overrides

Colors follow the [DaisyUI semantic token](https://daisyui.com/docs/colors/) model and are expressed as OKLCH values.

```csharp
options.AddTheme("Brand Theme", baseTheme: "jobmaster-light")
    .Primary("oklch(0.55 0.20 255)", content: "oklch(0.98 0 0)")
    .Secondary("oklch(0.50 0.18 300)", content: "oklch(0.98 0 0)")
    .Accent("oklch(0.52 0.16 195)", content: "oklch(0.98 0 0)")
    .BaseColors(
        base100: "oklch(0.98 0.005 270)",
        base200: "oklch(0.94 0.01 270)",
        base300: "oklch(0.88 0.02 270)",
        baseContent: "oklch(0.22 0.03 270)"
    );
```

---

## Border Radius Overrides

```csharp
options.AddTheme("Sharp Theme", baseTheme: "jobmaster-light")
    .SetBorderRadii(box: "0.25rem", btn: "0.25rem", badge: "0.125rem");
```

---

## Full Example

```csharp
builder.Services.AddJobMasterDashboard(options =>
{
    options.AddTheme("Corporate Light", baseTheme: "jobmaster-light")
        .MakePrimary()
        .DefaultForClusterIds("prod", "staging")
        .SetFontSans(
            ["Inter", "ui-sans-serif"],
            fontUrl: "https://fonts.googleapis.com/css2?family=Inter:wght@300;400;800&display=swap"
        )
        .Primary("oklch(0.45 0.18 240)", content: "oklch(0.98 0 0)")
        .SetBorderRadii(box: "0.5rem", btn: "0.375rem");

    options.AddTheme("Corporate Dark", baseTheme: "jobmaster-dark")
        .DefaultForClusterId("dev")
        .SetFontSans(
            ["Inter", "ui-sans-serif"],
            fontUrl: "https://fonts.googleapis.com/css2?family=Inter:wght@300;400;800&display=swap"
        );
});
```
