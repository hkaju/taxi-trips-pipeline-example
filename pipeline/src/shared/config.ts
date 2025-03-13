function validate(key: string): string {
  if (!process.env[key]) {
    throw Error(`Missing env: ${key}`);
  }
  return process.env[key] as string;
}

export default {
  DATA_INTAKE: validate("DATA_INTAKE"),
  POSTGRES_HOST: validate("POSTGRES_HOST"),
  POSTGRES_DB: validate("POSTGRES_DB"),
  POSTGRES_USER: validate("POSTGRES_USER"),
  POSTGRES_PASSWORD: validate("POSTGRES_PASSWORD"),
};
