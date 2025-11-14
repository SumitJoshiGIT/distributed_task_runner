/// <reference types="vite/client" />

declare interface ImportMetaEnv {
  readonly VITE_API_BASE?: string;
  readonly VITE_STRIPE_PUBLISHABLE_KEY?: string;
  readonly VITE_WALLET_SANDBOX?: string;
}

declare interface ImportMeta {
  readonly env: ImportMetaEnv;
}
