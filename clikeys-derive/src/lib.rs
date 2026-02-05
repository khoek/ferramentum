use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{quote, quote_spanned};
use syn::{
    Attribute, Data, DataStruct, DeriveInput, Fields, Lit, Type, parse_macro_input,
    spanned::Spanned,
};

#[proc_macro_derive(CliKeys, attributes(clikey))]
pub fn derive_cli_keys(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match impl_clikeys(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.into_compile_error().into(),
    }
}

struct FieldAttr {
    rename: Option<String>,
    help: Option<String>,
    ns: Option<String>,
    skip: bool,
}

fn parse_attrs(attrs: &[Attribute], field_name: &str) -> syn::Result<FieldAttr> {
    let mut out = FieldAttr {
        rename: None,
        help: None,
        ns: None,
        skip: false,
    };

    for attr in attrs {
        if !attr.path().is_ident("clikey") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(s) = value {
                    out.rename = Some(s.value());
                    Ok(())
                } else {
                    Err(syn::Error::new(value.span(), "rename must be string"))
                }
            } else if meta.path.is_ident("help") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(s) = value {
                    out.help = Some(s.value());
                    Ok(())
                } else {
                    Err(syn::Error::new(value.span(), "help must be string"))
                }
            } else if meta.path.is_ident("ns") {
                if meta.input.peek(syn::Token![=]) {
                    let value: Lit = meta.value()?.parse()?;
                    if let Lit::Str(s) = value {
                        out.ns = Some(s.value());
                        Ok(())
                    } else {
                        Err(syn::Error::new(value.span(), "ns must be string"))
                    }
                } else {
                    out.ns = Some(field_name.to_string());
                    Ok(())
                }
            } else if meta.path.is_ident("skip") {
                out.skip = true;
                Ok(())
            } else {
                Err(meta.error("unknown attribute"))
            }
        })?;
    }

    Ok(out)
}

fn is_leaf_type(ty: &Type) -> Option<&'static str> {
    match ty {
        Type::Path(tp) => {
            let last = tp.path.segments.last()?.ident.to_string();
            match last.as_str() {
                "bool" => Some("bool"),
                "usize" => Some("usize"),
                "u32" => Some("u32"),
                "u64" => Some("u64"),
                "i32" => Some("i32"),
                "i64" => Some("i64"),
                "f32" => Some("f32"),
                "f64" => Some("f64"),
                "String" => Some("String"),
                _ => None,
            }
        }
        _ => None,
    }
}

fn impl_clikeys(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;

    let ds = match &input.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(fields),
            ..
        }) => fields,
        _ => {
            return Err(syn::Error::new(
                input.span(),
                "#[derive(CliKeys)] supports only structs with named fields",
            ));
        }
    };

    let mut apply_stmts = Vec::new();
    let mut meta_stmts = Vec::new();

    for f in &ds.named {
        let ident = f.ident.as_ref().unwrap();
        let fspan = f.span();
        let FieldAttr {
            rename,
            help,
            ns,
            skip,
        } = parse_attrs(&f.attrs, &ident.to_string())?;

        if skip {
            continue;
        }

        let field_key = rename.unwrap_or_else(|| ident.to_string());
        let help_lit = help.unwrap_or_default();
        let fty = &f.ty;

        if let Some(tyname) = is_leaf_type(fty) {
            let tyname_str = syn::LitStr::new(tyname, Span::call_site());
            let key_lit = syn::LitStr::new(&field_key, Span::call_site());

            apply_stmts.push(quote_spanned! {fspan=>
                if key == #key_lit {
                    let parsed = <#fty as ::clikeys::ParseFromStr>::parse_str(value)
                        .map_err(|msg| ::clikeys::NsError::ParseError {
                            key: key.to_string(),
                            value: value.to_string(),
                            msg,
                        })?;
                    self.#ident = parsed;
                    return Ok(());
                }
            });

            meta_stmts.push(quote_spanned! {fspan=>
                meta.push(::clikeys::OptionMeta::with_default(
                    #key_lit,
                    #tyname_str,
                    #help_lit,
                    default.#ident.to_string()
                ));
            });
        } else {
            let ns_str = ns.unwrap_or_else(|| field_key.clone());
            let ns_lit = syn::LitStr::new(&ns_str, Span::call_site());

            apply_stmts.push(quote_spanned! {fspan=>
                if let Some((seg, rest)) = ::clikeys::split_once(key, '.') {
                    if seg == #ns_lit {
                        return ::clikeys::CliKeys::apply_kv(&mut self.#ident, rest, value);
                    }
                }
            });

            meta_stmts.push(quote_spanned! {fspan=>
                {
                    let child = <#fty as ::clikeys::CliKeys>::options_meta();
                    let child = ::clikeys::prefix_meta(#ns_lit, child);
                    meta.extend(child);
                }
            });
        }
    }

    apply_stmts.push(quote! {
        Err(::clikeys::NsError::UnknownKey(key.to_string()))
    });

    let tokens = quote! {
        impl ::clikeys::CliKeys for #name {
            fn options_meta() -> ::std::vec::Vec<::clikeys::OptionMeta> {
                let default: Self = <Self as ::std::default::Default>::default();
                let mut meta = ::std::vec::Vec::new();
                #(#meta_stmts)*
                meta
            }

            fn apply_kv(&mut self, key: &str, value: &str)
                -> ::std::result::Result<(), ::clikeys::NsError>
            {
                #(#apply_stmts)*
            }
        }

        impl #name {
            pub fn new_with_options<I, S>(options: I)
                -> ::std::result::Result<Self, ::clikeys::NsError>
            where
                I: ::std::iter::IntoIterator<Item = S>,
                S: ::std::convert::AsRef<str>,
            {
                let mut cfg: Self = ::std::default::Default::default();
                for opt in options {
                    let opt = opt.as_ref();
                    let Some((key, value)) = ::clikeys::split_once(opt, '=') else {
                        return Err(::clikeys::NsError::ParseError {
                            key: opt.to_string(),
                            value: ::std::string::String::new(),
                            msg: ::std::string::String::from("expected KEY=VALUE"),
                        });
                    };
                    ::clikeys::CliKeys::apply_kv(&mut cfg, key, value)?;
                }
                Ok(cfg)
            }
        }
    };

    Ok(tokens)
}
