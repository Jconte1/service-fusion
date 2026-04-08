type ServiceFusionEnv = {
  clientId: string;
  clientSecret: string;
};

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

export function getServiceFusionEnv(): ServiceFusionEnv {
  return {
    clientId: requireEnv("SERVICE_FUSION_CLIENT_ID"),
    clientSecret: requireEnv("SERVICE_FUSION_CLIENT_SECRET"),
  };
}
